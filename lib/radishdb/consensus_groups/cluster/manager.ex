use Croma
alias Croma.TypeGen, as: TG

defmodule RadishDB.ConsensusGroups.Cluster.Manager do
  @moduledoc """
  The Manager module is responsible for overseeing the activation and deactivation
  of consensus group members in the RadishDB system.

  It manages the lifecycle of consensus group nodes, including starting and stopping them,
  handling node reconnections, and coordinating leader elections.

  This module uses GenServer to handle asynchronous operations and maintain state
  regarding the consensus groups being managed.

  ## Functions

  - `start_link/0`: Starts the Manager process.
  - `activate/1`: Activates a node in a specified zone.
  - `deactivate/0`: Deactivates the current node.
  - `start_consensus_group_members/4`: Starts the members of a consensus group.
  - `start_consensus_group_follower/4`: Starts a follower for a consensus group.
  """

  use GenServer
  require Logger

  alias RadishDB.ConsensusGroups.API
  alias RadishDB.ConsensusGroups.Cluster.ConsensusMemberAdjuster
  alias RadishDB.ConsensusGroups.Config.Config
  alias RadishDB.ConsensusGroups.OTP.{ConsensusMemberSupervisor, ProcessAndDiskLogIndexInspector}
  alias RadishDB.Raft.Node, as: RaftNode
  alias RadishDB.Raft.Types.Config, as: RaftConfig
  alias RadishDB.Raft.Types.Error.AddFollowerError

  defmodule Activator do
    @moduledoc """
    The Activator module handles the activation process for nodes in the consensus group.
    """

    alias RadishDB.ConsensusGroups.Cluster.Cluster
    alias RadishDB.ConsensusGroups.OTP.NodeReconnect

    @tries 5
    @sleep 1_000

    @doc """
    Activates a node in the specified zone by executing a series of steps.
    """
    def activate(zone) do
      steps = [
        :start_cluster_consensus_member,
        {:add_node, zone},
        :notify_node_reconnect_in_this_node,
        :notify_node_reconnects_in_other_nodes
      ]

      run_steps(steps, @tries)
    end

    defp run_steps(_, 0), do: raise("Failed to complete all steps of node activation!")
    defp run_steps([], _), do: :ok

    defp run_steps([s | ss], tries_remaining) do
      case step(s) do
        :ok ->
          run_steps(ss, tries_remaining)

        :error ->
          :timer.sleep(@sleep)
          run_steps([s | ss], tries_remaining - 1)
      end
    end

    defp step(:start_cluster_consensus_member) do
      case Supervisor.start_child(API.Supervisor, Cluster.Server.child_spec()) do
        {:ok, _} -> :ok
        {:error, _} -> :error
      end
    end

    defp step({:add_node, zone}) do
      case API.command(Cluster, {:add_node, Node.self(), zone}) do
        {:ok, _} -> :ok
        {:error, _} -> :error
      end
    end

    defp step(:notify_node_reconnect_in_this_node) do
      GenServer.cast(NodeReconnect, :this_node_activated)
    end

    defp step(:notify_node_reconnects_in_other_nodes) do
      GenServer.abcast(Node.list(), NodeReconnect, {:other_node_activated, Node.self()})
      :ok
    end
  end

  defmodule Deactivator do
    @moduledoc """
    The Deactivator module handles the deactivation process for nodes in the consensus group.
    """

    require Logger

    alias RadishDB.ConsensusGroups.Cache.LeaderPidCache
    alias RadishDB.ConsensusGroups.Cluster.Cluster
    alias RadishDB.ConsensusGroups.OTP.NodeReconnect
    alias RadishDB.Raft.Node, as: RaftNode

    @tries 10
    @sleep 1_000
    @deactivate_steps [
      :remove_node_command,
      :remove_follower_from_cluster_consensus,
      :delete_child_from_supervisor,
      :notify_node_reconnect_in_this_node
    ]

    @doc """
    Deactivates the current node by executing a series of steps.
    """
    def deactivate do
      run_steps(@deactivate_steps, @tries)
    end

    defp run_steps(_, 0), do: raise("Failed to complete all steps of node deactivation!")
    defp run_steps([], _), do: :ok

    defp run_steps([s | ss], tries_remaining) do
      case step(s) do
        :ok ->
          run_steps(ss, tries_remaining)

        :error ->
          :timer.sleep(@sleep)
          run_steps([s | ss], tries_remaining - 1)
      end
    end

    defp step(:remove_node_command) do
      case API.command(Cluster, {:remove_node, Node.self()}) do
        {:ok, _} -> :ok
        _ -> :error
      end
    end

    defp step(:remove_follower_from_cluster_consensus) do
      local_member = Process.whereis(Cluster)

      case LeaderPidCache.find_leader_and_cache(Cluster) do
        nil -> :error
        ^local_member -> handle_current_leader(local_member)
        current_leader -> remove_follower(current_leader, local_member)
      end
    end

    defp step(:delete_child_from_supervisor) do
      :ok = Supervisor.terminate_child(API.Supervisor, Cluster.Server)
      :ok = Supervisor.delete_child(API.Supervisor, Cluster.Server)
    end

    defp step(:notify_node_reconnect_in_this_node) do
      GenServer.cast(NodeReconnect, :this_node_deactivated)
    end

    defp handle_current_leader(local_member) do
      status = RaftNode.status(local_member)
      other_members = List.delete(status[:members], local_member)

      if other_members == [] do
        :ok
      else
        case pick_next_leader(local_member, other_members) do
          # No suitable member found
          nil -> :error
          next_leader -> replace_leader(local_member, next_leader)
        end
      end
    end

    defp remove_follower(current_leader, local_member) do
      case catch_exit(fn -> RaftNode.remove_follower(current_leader, local_member) end) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.error("remove follower failed: #{inspect(reason)}")
          :error
      end
    end

    defp pick_next_leader(current_leader, other_members) do
      # We don't want to migrate the current leader to an inactive node;
      # check currently active nodes before choosing a member.
      case RaftNode.query(current_leader, :active_nodes) do
        {:ok, nodes_per_zone} ->
          nodes = Map.values(nodes_per_zone) |> List.flatten() |> MapSet.new()

          case Enum.filter(other_members, &(node(&1) in nodes)) do
            [] -> nil
            members_in_active_nodes -> Enum.random(members_in_active_nodes)
          end

        {:error, _} ->
          # Although local member has been the leader until very recently,
          # it turns out that it's not leader now.
          # Let's retry from the beginning of the step.
          nil
      end
    end

    defp replace_leader(leader, next_leader) do
      catch_exit(fn -> RaftNode.replace_leader(leader, next_leader) end)
      |> case do
        :ok ->
          Logger.info(
            "replaced current leader (#{inspect(leader)}) in this node with #{inspect(next_leader)} in #{node(next_leader)} to deactivate this node"
          )

          LeaderPidCache.set(Cluster, next_leader)

        {:error, reason} ->
          Logger.error(
            "tried to replace current leader in this node but failed: #{inspect(reason)}"
          )
      end
    end

    defp catch_exit(f) do
      f.()
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defmodule State do
    @moduledoc """
    The State module maintains the internal state of the Manager process.
    """

    @type consensus_group_progress ::
            :leader_started | {:leader_delegated_to, node} | :process_exists | GenServer.from()

    use Croma.Struct,
      fields: [
        adjust_timer: TG.nilable(Croma.Reference),
        adjust_worker: TG.nilable(Croma.Pid),
        activate_worker: TG.nilable(Croma.Pid),
        deactivate_worker: TG.nilable(Croma.Pid),
        # %{atom => consensus_group_progress}
        being_added_consensus_groups: Croma.Map
      ]

    @doc """
    Determines the current phase of the Manager based on its state.
    """
    def phase(%__MODULE__{adjust_timer: t, activate_worker: a, deactivate_worker: d}) do
      cond do
        t -> :active
        a -> :activating
        d -> :deactivating
        true -> :inactive
      end
    end

    @doc """
    Updates the state with the progress of adding a consensus group.
    """
    defun update_being_added_consensus_groups(
            %__MODULE__{being_added_consensus_groups: gs} = s,
            name :: atom,
            value2 :: consensus_group_progress
          ) :: t do
      case gs[name] do
        nil ->
          %__MODULE__{s | being_added_consensus_groups: Map.put(gs, name, value2)}

        value1 ->
          case {gen_server_from?(value1), gen_server_from?(value2)} do
            {true, false} ->
              GenServer.reply(value1, convert_to_reply(value2))
              %__MODULE__{s | being_added_consensus_groups: Map.delete(gs, name)}

            {false, true} ->
              GenServer.reply(value2, convert_to_reply(value1))
              %__MODULE__{s | being_added_consensus_groups: Map.delete(gs, name)}

            _ ->
              %__MODULE__{s | being_added_consensus_groups: Map.put(gs, name, value2)}
          end
      end
    end

    defp gen_server_from?({p, r}) when is_pid(p), do: ref_or_alias_ref?(r)
    defp gen_server_from?(_), do: false

    defp ref_or_alias_ref?([:alias | r]), do: is_reference(r)
    defp ref_or_alias_ref?(r), do: is_reference(r)

    defp convert_to_reply(:leader_started), do: {:ok, :leader_started}
    defp convert_to_reply({:leader_delegated_to, node}), do: {:ok, {:leader_delegated_to, node}}
    defp convert_to_reply(:process_exists), do: {:error, :process_exists}
  end

  @doc """
  Starts the Manager process.
  """
  defun start_link([]) :: GenServer.on_start() do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Initializes the Manager state.
  """
  def init(:ok) do
    {:ok, %State{being_added_consensus_groups: %{}}}
  end

  @doc """
  Activates a node in the specified zone.
  """
  def handle_call({:activate, zone}, _from, state) do
    if State.phase(state) == :inactive do
      new_state = %State{state | activate_worker: start_worker(Activator, :activate, [zone])}
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :not_inactive}, state}
    end
  end

  def handle_call(:deactivate, _from, state) do
    if State.phase(state) == :active do
      new_state =
        %State{state | deactivate_worker: start_worker(Deactivator, :deactivate, [])}
        |> stop_timer()

      {:reply, :ok, new_state}
    else
      {:reply, {:error, :inactive}, state}
    end
  end

  def handle_call({:await_completion_of_adding_consensus_group, name}, from, state) do
    if State.phase(state) in [:active, :activating] do
      {:noreply, State.update_being_added_consensus_groups(state, name, from)}
    else
      {:reply, {:error, :inactive}, state}
    end
  end

  def handle_cast({:start_consensus_group_members, name, raft_config, member_nodes}, state) do
    if State.phase(state) in [:active, :activating] do
      handle_active_state(name, raft_config, member_nodes, state)
    else
      Logger.info(
        "manager process is not active; cannot start member processes for consensus group #{name}"
      )

      {:noreply, state}
    end
  end

  def handle_cast({:start_consensus_group_follower, name, leader_node_hint}, state) do
    if State.phase(state) == :active do
      other_node_list =
        case leader_node_hint do
          nil -> Node.list()
          # reorder nodes so that the new follower can find leader immediately (most of the time)
          node -> [node | List.delete(Node.list(), node)]
        end

      other_node_members = Enum.map(other_node_list, fn n -> {name, n} end)

      # To avoid blocking the Manager process, we spawn a temporary process solely for `:supervisor.start_child/2`.
      spawn_link(fn -> start_follower_with_retry(other_node_members, name, 3) end)
    end

    {:noreply, state}
  end

  defp handle_active_state(name, raft_config, member_nodes, state) do
    case ProcessAndDiskLogIndexInspector.find_node_having_latest_log_index(name) do
      {:ok, node_or_nil} ->
        node_to_host_initial_leader = node_or_nil || Node.self()

        start_or_delegate_leader(
          node_to_host_initial_leader,
          name,
          raft_config,
          member_nodes,
          state
        )

      {:error, :process_exists} ->
        {:noreply, State.update_being_added_consensus_groups(state, name, :process_exists)}
    end
  end

  defp start_or_delegate_leader(
         node_to_host_initial_leader,
         name,
         raft_config,
         member_nodes,
         state
       ) do
    if node_to_host_initial_leader == Node.self() do
      start_leader_and_tell_other_nodes_to_start_follower(name, raft_config, member_nodes, state)
      {:noreply, state}
    else
      start_consensus_group_members(
        {__MODULE__, node_to_host_initial_leader},
        name,
        raft_config,
        member_nodes
      )

      new_state =
        State.update_being_added_consensus_groups(
          state,
          name,
          {:leader_delegated_to, node_to_host_initial_leader}
        )

      {:noreply, new_state}
    end
  end

  defp start_leader_and_tell_other_nodes_to_start_follower(name, raft_config, member_nodes, state) do
    additional_args = [{:create_new_consensus_group, raft_config}, name]

    case :supervisor.start_child(ConsensusMemberSupervisor, additional_args) do
      {:ok, _pid} ->
        tell_other_nodes_to_start_followers_with_delay(name, member_nodes)
        State.update_being_added_consensus_groups(state, name, :leader_started)

      {:error, reason} ->
        Logger.info(
          "error in starting 1st member process of consensus group #{name}: #{inspect(reason)}"
        )

        state
    end
  end

  defp tell_other_nodes_to_start_followers_with_delay(name, member_nodes) do
    # Concurrently spawning multiple followers may lead to race conditions
    # (adding a node can only be done one-by-one).
    # Although this race condition can be automatically resolved by retries and thus is harmless,
    # we should try to minimize amount of unnecessary error logs.
    List.delete(member_nodes, Node.self())
    |> Enum.with_index()
    |> Enum.each(fn {node, i} ->
      start_consensus_group_follower(
        name,
        node,
        Node.self(),
        i * Config.follower_addition_delay()
      )
    end)
  end

  defp start_follower_with_retry(_, name, 0) do
    Logger.error("give up adding follower for #{name} due to consecutive failures")
    {:error, :cannot_start_child}
  end

  defp start_follower_with_retry(other_node_members, name, tries_remaining) do
    case :supervisor.start_child(ConsensusMemberSupervisor, [
           {:join_existing_consensus_group, other_node_members},
           name
         ]) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> handle_follower_error(reason, other_node_members, name, tries_remaining)
    end
  end

  defp handle_follower_error(reason, other_node_members, name, tries_remaining) do
    case extract_dead_follower_pid_from_reason(reason) do
      nil -> :ok
      dead_follower_pid -> spawn(fn -> try_to_remove_dead_follower(name, dead_follower_pid) end)
    end

    :timer.sleep(2 * Config.follower_addition_delay())

    if tries_remaining > 1 do
      start_follower_with_retry(other_node_members, name, tries_remaining - 1)
    else
      :error
    end
  end

  defp extract_dead_follower_pid_from_reason(%AddFollowerError{pid: pid}), do: pid

  defp extract_dead_follower_pid_from_reason(
         {:timeout, {_mod, _fun, [_server, {:add_follower, pid}, _timeout]}}
       ),
       do: pid

  defp extract_dead_follower_pid_from_reason(_), do: nil

  defp try_to_remove_dead_follower(name, dead_follower_pid) do
    case API.whereis_leader(name) do
      # give up
      nil ->
        :ok

      leader ->
        # reduce possibility of race condition: `:uncommitted_membership_change`
        :timer.sleep(Config.follower_addition_delay())
        RaftNode.remove_follower(leader, dead_follower_pid)
    end
  end

  def handle_info(:adjust_members, %State{adjust_worker: worker} = state0) do
    state1 =
      if worker do
        # don't invoke multiple workers
        state0
      else
        %State{state0 | adjust_worker: start_worker(ConsensusMemberAdjuster, :adjust, [])}
      end

    state2 =
      if State.phase(state1) == :active do
        start_timer(state1)
      else
        state1
      end

    {:noreply, state2}
  end

  def handle_info({:DOWN, _ref, :process, pid, info}, %State{activate_worker: pid} = state) do
    log_abnormal_exit_reason(info, :activate)
    {:noreply, start_timer(%State{state | activate_worker: nil})}
  end

  def handle_info({:DOWN, _ref, :process, pid, info}, %State{deactivate_worker: pid} = state) do
    log_abnormal_exit_reason(info, :deactivate)
    {:noreply, %State{state | deactivate_worker: nil}}
  end

  def handle_info({:DOWN, _ref, :process, pid, info}, %State{adjust_worker: pid} = state) do
    log_abnormal_exit_reason(info, :adjust)
    {:noreply, %State{state | adjust_worker: nil}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp start_timer(%State{adjust_timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)

    %State{
      state
      | adjust_timer: Process.send_after(self(), :adjust_members, Config.balancing_interval())
    }
  end

  defp stop_timer(%State{adjust_timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    %State{state | adjust_timer: nil}
  end

  defp start_worker(mod, fun, args) do
    {pid, _} = spawn_monitor(mod, fun, args)
    pid
  end

  defp log_abnormal_exit_reason(:normal, _), do: :ok

  defp log_abnormal_exit_reason(reason, worker_type) do
    Logger.error("#{worker_type} worker died unexpectedly: #{inspect(reason)}")
  end

  @doc """
  Starts the consensus group members in the specified server.
  """
  defun start_consensus_group_members(
          server :: GenServer.server() \\ __MODULE__,
          name :: atom,
          raft_config :: RaftConfig.t(),
          member_nodes :: [node]
        ) :: :ok do
    GenServer.cast(server, {:start_consensus_group_members, name, raft_config, member_nodes})
  end

  @doc """
  Starts a follower for the specified consensus group with an optional delay.
  """
  defun start_consensus_group_follower(
          name :: atom,
          node :: node,
          leader_node_hint :: nil | node,
          delay :: non_neg_integer \\ 0
        ) :: :ok do
    send_fun = fn ->
      GenServer.cast(
        {__MODULE__, node},
        {:start_consensus_group_follower, name, leader_node_hint}
      )
    end

    if delay == 0 do
      send_fun.()
    else
      spawn(fn ->
        :timer.sleep(delay)
        send_fun.()
      end)
    end

    :ok
  end
end
