use Croma

defmodule RadishDB.ConsensusGroups.Cluster.Cluster do
  @moduledoc """
  This module implements a distributed consensus algorithm using the Raft protocol.

  It manages the lifecycle of consensus groups, including starting new servers and handling node states.
  The module consists of several submodules:

  - `Server`: Manages the server lifecycle and handles race conditions.
  - `State`: Maintains the cluster's state, including nodes, consensus groups, and recently removed groups.
  - `Hook`: Implements callbacks for leader election and command commitment.
  """

  alias RadishDB.ConsensusGroups.Cluster.{Manager, RecentlyRemovedGroups}
  alias RadishDB.ConsensusGroups.Config.{Config, PerMemberOptions}
  alias RadishDB.ConsensusGroups.Types.{ConsensusGroups, MembersPerLeaderNode}
  alias RadishDB.ConsensusGroups.Utils.NodesPerZone
  alias RadishDB.Raft.Node, as: RaftNode
  alias RadishDB.Raft.StateMachine.Statable
  alias RadishDB.Raft.Types.Config, as: RaftConfig

  defmodule Server do
    @moduledoc """
    The Server module handles the lifecycle of the consensus group servers.

    It provides functions to start new servers, check for existing servers, and manage retries
    in case of server failures or race conditions.
    """

    alias RadishDB.ConsensusGroups.Cluster.Cluster
    alias RadishDB.ConsensusGroups.Config.Config

    defun start_link(raft_config :: RaftConfig.t(), name :: g[atom]) :: GenServer.on_start() do
      # Use lock facility provided by :global module to avoid race conditions
      :global.trans(
        {:radish_db_cluster_state_initialization, self()},
        fn ->
          if not Enum.any?(Node.list(), fn n -> raft_server_alive?({name, n}) end) do
            RaftNode.start_link(
              {:create_new_consensus_group, raft_config},
              PerMemberOptions.build(name)
            )
          end
        end,
        [Node.self() | Node.list()],
        0
      )
      |> case do
        {:ok, pid} ->
          {:ok, pid}

        _ ->
          # Other server exists or cannot acquire lock; we need to retry since there may be no leader
          start_follower_with_retry(name, 3)
      end
    end

    defunp raft_server_alive?(server :: {atom, node}) :: boolean do
      try do
        _ = RaftNode.status(server)
        true
      catch
        :exit, {:noproc, _} -> false
      end
    end

    defunp start_follower_with_retry(name :: atom, tries_remaining :: non_neg_integer) ::
             GenServer.on_start() do
      if tries_remaining == 0 do
        {:error, :no_leader}
      else
        servers = Node.list() |> Enum.map(fn n -> {name, n} end)

        case RaftNode.start_link(
               {:join_existing_consensus_group, servers},
               PerMemberOptions.build(name)
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, _} ->
            :timer.sleep(1_000)
            start_follower_with_retry(name, tries_remaining - 1)
        end
      end
    end

    defun child_spec() :: Supervisor.child_spec() do
      raft_config =
        case Config.raft_config() do
          nil -> Cluster.make_raft_config()
          mod -> mod.make(Cluster)
        end

      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, [raft_config, Cluster]},
        type: :worker,
        restart: :transient,
        shutdown: 5000
      }
    end
  end

  defmodule State do
    @moduledoc """
    The State module maintains the current state of the consensus cluster.

    It tracks nodes, consensus groups, recently removed groups, and members per leader node.
    It provides functions to add or remove nodes and groups.
    """

    use Croma.Struct,
      fields: [
        nodes_per_zone: NodesPerZone,
        consensus_groups: ConsensusGroups,
        recently_removed_groups: RecentlyRemovedGroups,
        # this is cache; reproducible from `nodes` and `consensus_groups`
        members_per_leader_node: MembersPerLeaderNode
      ]

    def add_group(state, group, n_replica) do
      %__MODULE__{
        nodes_per_zone: nodes,
        consensus_groups: groups,
        recently_removed_groups: rrgs,
        members_per_leader_node: members
      } = state

      cond do
        Map.has_key?(groups, group) ->
          {{:error, :already_added}, state}

        Enum.empty?(nodes) ->
          {{:error, :no_active_node}, state}

        RecentlyRemovedGroups.cleanup_ongoing?(rrgs, group) ->
          {{:error, :cleanup_ongoing}, state}

        true ->
          create_new_group(state, group, n_replica, nodes, members)
      end
    end

    defp create_new_group(state, group, n_replica, nodes, members) do
      member_nodes = NodesPerZone.lrw_members(nodes, group, n_replica)
      [leader | _] = member_nodes
      pair = {group, member_nodes}

      new_members = Map.update(members, leader, [pair], &[pair | &1])
      new_groups = Map.put(state.consensus_groups, group, n_replica)

      new_state = %__MODULE__{
        state
        | consensus_groups: new_groups,
          members_per_leader_node: new_members
      }

      {{:ok, member_nodes}, new_state}
    end

    def remove_group(state, group) do
      %__MODULE__{
        nodes_per_zone: nodes,
        consensus_groups: groups,
        recently_removed_groups: rrgs,
        members_per_leader_node: members
      } = state

      if Map.has_key?(groups, group) do
        new_state = update_groups_and_rrgs(state, groups, rrgs, group)

        if map_size(nodes) == 0 do
          {:ok, new_state}
        else
          update_members(new_state, nodes, members, group)
        end
      else
        {{:error, :not_found}, state}
      end
    end

    defp update_groups_and_rrgs(state, groups, rrgs, group) do
      new_groups = Map.delete(groups, group)
      new_rrgs = RecentlyRemovedGroups.add(rrgs, group)

      %__MODULE__{
        state
        | consensus_groups: new_groups,
          recently_removed_groups: new_rrgs
      }
    end

    defp update_members(new_state, nodes, members, group) do
      [leader] = NodesPerZone.lrw_members(nodes, group, 1)
      new_group_members_pairs = members[leader] |> Enum.reject(&match?({^group, _}, &1))

      new_members =
        if new_group_members_pairs == [] do
          Map.delete(members, leader)
        else
          Map.put(members, leader, new_group_members_pairs)
        end

      {:ok, %__MODULE__{new_state | members_per_leader_node: new_members}}
    end

    def add_node(state, n, z) do
      %__MODULE__{nodes_per_zone: nodes, consensus_groups: groups} = state
      new_nodes = Map.update(nodes, z, [n], fn ns -> Enum.uniq([n | ns]) end)

      %__MODULE__{
        state
        | nodes_per_zone: new_nodes,
          members_per_leader_node: compute_members(new_nodes, groups)
      }
    end

    def remove_node(state, n) do
      %__MODULE__{nodes_per_zone: nodes, consensus_groups: groups} = state

      new_nodes =
        Enum.reduce(nodes, %{}, fn {z, ns}, m ->
          case ns do
            [^n] -> m
            _ -> Map.put(m, z, List.delete(ns, n))
          end
        end)

      new_members = compute_members(new_nodes, groups)
      %__MODULE__{state | nodes_per_zone: new_nodes, members_per_leader_node: new_members}
    end

    defp compute_members(nodes, groups) do
      if map_size(nodes) == 0 do
        %{}
      else
        Enum.map(groups, fn {group, n_replica} ->
          {group, NodesPerZone.lrw_members(nodes, group, n_replica)}
        end)
        |> Enum.group_by(fn {_, members} -> hd(members) end)
      end
    end

    def update_removed_groups(state, node, index_or_nil, now, wait_time) do
      %__MODULE__{nodes_per_zone: npz, recently_removed_groups: rrgs} = state
      new_rrgs = RecentlyRemovedGroups.update(rrgs, npz, node, index_or_nil, now, wait_time)
      %__MODULE__{state | recently_removed_groups: new_rrgs}
    end
  end

  defmodule Hook do
    @moduledoc """
    The Hook module implements callbacks for leader election and command commitment.

    It ensures that consensus groups are started appropriately based on the leader's state
    and handles the restoration of groups from logs and snapshots.
    """

    alias RadishDB.ConsensusGroups.Cluster.Manager
    alias RadishDB.ConsensusGroups.Config.Config
    alias RadishDB.Raft.Communication.LeaderHook

    @behaviour LeaderHook

    @impl true
    def on_command_committed(_state_before, entry, ret, _state_after) do
      case entry do
        {:add_group, group_name, _, raft_config, leader_node} ->
          handle_add_group_command(leader_node, ret, group_name, raft_config)

        _ ->
          nil
      end
    end

    defp handle_add_group_command(leader_node, ret, group_name, raft_config) do
      if Node.self() == leader_node do
        case ret do
          {:error, _} ->
            nil

          {:ok, nodes} ->
            start_consensus_group_if_not_restoring(group_name, raft_config, nodes)
        end
      end
    end

    defp start_consensus_group_if_not_restoring(group_name, raft_config, nodes) do
      restoring? = Process.get(:radish_db_raft_rpc_server_restoring, false)

      unless restoring? do
        Manager.start_consensus_group_members(group_name, raft_config, nodes)
      end
    end

    @impl true
    def on_query_answered(_, _, _), do: nil

    @impl true
    def on_follower_added(_, _), do: nil

    @impl true
    def on_follower_removed(_, _), do: nil

    @impl true
    def on_elected(_), do: nil

    @impl true
    def on_restored_from_files(%State{consensus_groups: gs}) do
      case Config.raft_config() do
        nil ->
          :ok

        mod ->
          # As it's right after 1st activation,
          # it's highly likely that the cluster consists of only 1 node;
          # let's start consensus groups with 1 member.
          target_nodes = [Node.self()]

          Enum.each(gs, fn {g, _} ->
            Manager.start_consensus_group_members(g, mod.make(g), target_nodes)
          end)
      end
    end
  end

  @behaviour Statable
  @typep t :: State.t()

  @impl true
  defun new() :: t do
    %State{
      nodes_per_zone: %{},
      consensus_groups: %{},
      recently_removed_groups: RecentlyRemovedGroups.empty(),
      members_per_leader_node: %{}
    }
  end

  @impl true
  defun command(data :: t, arg :: Statable.command_arg()) :: {Statable.command_ret(), t} do
    # `raft_config` and `node` will be used in `Hook`
    data, {:add_group, group, n, _raft_config, _node} ->
      State.add_group(data, group, n)

    data, {:remove_group, group} ->
      State.remove_group(data, group)

    data, {:add_node, node, zone} ->
      {:ok, State.add_node(data, node, zone)}

    data, {:remove_node, node} ->
      {:ok, State.remove_node(data, node)}

    data, {:stopped_extra_members, node, index, now, wait} ->
      {:ok, State.update_removed_groups(data, node, index, now, wait)}

    # For safety
    data, _ ->
      {{:error, :invalid_command}, data}
  end

  @impl true
  defun query(data :: t, arg :: Statable.query_arg()) :: Statable.query_ret() do
    %State{
      nodes_per_zone: nodes,
      members_per_leader_node: members,
      recently_removed_groups: removed
    },
    {:consensus_groups, node} ->
      participating_nodes = Enum.flat_map(nodes, fn {_z, ns} -> ns end)
      groups_led_by_the_node = Map.get(members, node, [])
      removed_groups_with_index = RecentlyRemovedGroups.names_for_node(removed, node)
      {participating_nodes, groups_led_by_the_node, removed_groups_with_index}

    %State{nodes_per_zone: nodes}, :active_nodes ->
      nodes

    %State{consensus_groups: groups}, :consensus_groups ->
      groups

    # For safety
    _, _ ->
      {:error, :invalid_query}
  end

  @default_raft_config_options [
    election_timeout_clock_drift_margin: 500,
    leader_hook_module: Hook
  ]

  defun make_raft_config(raft_config_options :: Keyword.t() \\ @default_raft_config_options) ::
          RaftConfig.t() do
    # :leader_hook_module must not be changed
    opts = Keyword.put(raft_config_options, :leader_hook_module, Hook)
    RaftNode.make_config(__MODULE__, opts)
  end
end
