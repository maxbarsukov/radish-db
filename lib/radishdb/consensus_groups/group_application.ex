use Croma

defmodule RadishDB.ConsensusGroups.GroupApplication do
  @moduledoc """
  Public interface functions of consensus groups managing.
  """

  use Application
  alias RadishDB.ConsensusGroups.Cache.LeaderPidCache
  alias RadishDB.ConsensusGroups.Config.Config
  alias RadishDB.ConsensusGroups.Cluster.{Cluster, Manager}
  alias RadishDB.ConsensusGroups.GroupApplication, as: App

  alias RadishDB.ConsensusGroups.OTP.{
    ConsensusMemberSupervisor,
    LeaderPidCacheRefresher,
    NodeReconnect,
    ProcessAndDiskLogIndexInspector
  }

  alias RadishDB.ConsensusGroups.Types.ZoneId
  alias RadishDB.Raft.Node, as: RaftNode
  alias RadishDB.Raft.StateMachine.Statable
  alias RadishDB.Raft.Types.Config, as: RaftConfig

  @impl true
  def start(_type, _args) do
    LeaderPidCache.init()

    children = [
      ConsensusMemberSupervisor,
      Manager,
      NodeReconnect,
      LeaderPidCacheRefresher,
      ProcessAndDiskLogIndexInspector
    ]

    opts = [strategy: :one_for_one, name: App.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def stop(_state) do
    :ets.delete(:leader_pid_cache)
  end

  @default_timeout 500
  @default_retry 3
  @default_retry_interval 1_000

  @doc """
  Activates `Node.self()`.
  """
  defun activate(zone :: ZoneId.t()) :: :ok | {:error, :not_inactive} do
    GenServer.call(Manager, {:activate, zone})
  end

  @doc """
  Deactivates `Node.self()`.
  """
  defun deactivate() :: :ok | {:error, :inactive} do
    GenServer.call(Manager, :deactivate)
  end

  @doc """
  Queries the current nodes which have been activated using `activate/1` in the cluster.
  """
  defun active_nodes() :: %{ZoneId.t() => [node]} do
    {:ok, ret} = App.query(Cluster, :active_nodes)
    ret
  end

  @doc """
  Registers a new consensus group identified by `name`.
  """
  defun add_consensus_group(name :: g[atom]) ::
          :ok | {:error, :already_added | :cleanup_ongoing | :no_leader | any} do
    case Config.raft_config() do
      nil -> raise "No module is specified as `:raft_config` option!"
      mod -> add_consensus_group(name, 3, mod.make(name))
    end
  end

  @doc """
  Registers a new consensus group identified by `name`.
  """
  defun add_consensus_group(
          name :: g[atom],
          n_replica :: g[pos_integer],
          raft_config = %RaftConfig{},
          await_timeout :: g[pos_integer] \\ 5_000
        ) :: :ok | {:error, :already_added | :cleanup_ongoing | :no_leader | any} do
    ref = make_ref()

    call_with_retry(Cluster, @default_retry + 1, @default_retry_interval, fn pid ->
      leader_node = node(pid)
      command_arg = {:add_group, name, n_replica, raft_config, leader_node}

      case RaftNode.command(pid, command_arg, @default_timeout, ref) do
        {:ok, r} -> {:ok, {leader_node, r}}
        {:error, _} = e -> e
      end
    end)
    |> case do
      {:ok, {leader_node, {:ok, _nodes}}} ->
        try do
          msg = {:await_completion_of_adding_consensus_group, name}

          case GenServer.call({Manager, leader_node}, msg, await_timeout) do
            {:ok, :leader_started} ->
              :ok

            {:ok, {:leader_delegated_to, delegated_node}} ->
              {:ok, :leader_started} =
                GenServer.call({Manager, delegated_node}, msg, await_timeout)

              :ok

            {:error, :process_exists} ->
              # rollback the newly-added consensus group
              remove_consensus_group(name)
              {:error, :cleanup_ongoing}
          end
        catch
          :exit, reason -> {:error, reason}
        end

      {:ok, {_leader_node, {:error, _reason} = e}} ->
        e

      {:error, :no_leader} = e ->
        e
    end
  end

  @doc """
  Removes an existing consensus group identified by `name`.
  """
  defun remove_consensus_group(name :: g[atom]) :: :ok | {:error, :not_found | :no_leader} do
    case command(Cluster, {:remove_group, name}) do
      {:ok, ret} -> ret
      error -> error
    end
  end

  @doc """
  Queries already registered consensus groups.
  """
  defun consensus_groups() :: %{atom => pos_integer} do
    {:ok, ret} = App.query(Cluster, :consensus_groups)
    ret
  end

  @doc """
  Executes a command on the replicated value identified by `name`.
  """
  defun command(
          name :: g[atom],
          command_arg :: Statable.command_arg(),
          timeout :: g[pos_integer] \\ @default_timeout,
          retry :: g[non_neg_integer] \\ @default_retry,
          retry_interval :: g[pos_integer] \\ @default_retry_interval,
          call_module :: g[module] \\ :gen_statem
        ) :: {:ok, Statable.command_ret()} | {:error, :no_leader} do
    ref = make_ref()

    call_with_retry(name, retry + 1, retry_interval, fn pid ->
      RaftNode.command(pid, command_arg, timeout, ref, call_module)
    end)
  end

  @doc """
  Executes a read-only query on the replicated value identified by `name`.
  """
  defun query(
          name :: g[atom],
          query_arg :: Statable.query_arg(),
          timeout :: g[pos_integer] \\ @default_timeout,
          retry :: g[non_neg_integer] \\ @default_retry,
          retry_interval :: g[pos_integer] \\ @default_retry_interval,
          call_module :: g[module] \\ :gen_statem
        ) :: {:ok, Statable.query_ret()} | {:error, :no_leader} do
    call_with_retry(name, retry + 1, retry_interval, fn pid ->
      RaftNode.query(pid, query_arg, timeout, call_module)
    end)
  end

  defp call_with_retry(name, tries_remaining, retry_interval, f) do
    if tries_remaining == 0 do
      {:error, :no_leader}
    else
      case LeaderPidCache.get(name) do
        nil -> find_leader_and_exec(name, tries_remaining, retry_interval, f)
        leader_pid -> execute_with_retry(leader_pid, name, tries_remaining, retry_interval, f)
      end
    end
  end

  defp find_leader_and_exec(name, tries_remaining, retry_interval, f) do
    case LeaderPidCache.find_leader_and_cache(name) do
      nil -> retry(name, tries_remaining, retry_interval, f)
      leader_pid -> execute_with_retry(leader_pid, name, tries_remaining, retry_interval, f)
    end
  end

  defp execute_with_retry(leader_pid, name, tries_remaining, retry_interval, f) do
    case run_with_catch(leader_pid, f) do
      {:ok, _} = ok -> ok
      {:error, _} -> retry(name, tries_remaining, retry_interval, f)
    end
  end

  defp run_with_catch(pid, f) do
    f.(pid)
  catch
    :exit, _ -> {:error, :exit}
  end

  defp retry(name, tries_remaining, retry_interval, f) do
    :timer.sleep(retry_interval)
    call_with_retry(name, tries_remaining - 1, retry_interval, f)
  end

  @doc """
  Tries to find the current leader of the consensus group specified by `name`.
  """
  defun whereis_leader(name :: g[atom]) :: nil | pid do
    case LeaderPidCache.get(name) do
      nil -> LeaderPidCache.find_leader_and_cache(name)
      pid -> pid
    end
  end

  @doc """
  Inspects members of consensus groups and finds a group (if any) in which no leader exists.
  """
  defun find_consensus_group_with_no_established_leader() ::
          :ok | {group_name :: atom, [{node, map}]} do
    case inspect_statuses_of_consensus_group(Cluster) do
      {:error, pairs} ->
        {Cluster, pairs}

      {:ok, _} ->
        App.consensus_groups()
        |> Enum.find_value(:ok, fn {group, _n_desired_members} ->
          case inspect_statuses_of_consensus_group(group) do
            {:ok, _} -> nil
            {:error, pairs} -> {group, pairs}
          end
        end)
    end
  end

  @doc """
  Removes member pids that reside in the specified dead node from all existing consensus groups.
  """
  defun remove_dead_pids_located_in_dead_node(dead_node :: g[node]) :: :ok do
    remove_pid_located_in_dead_node_from_consensus_group(Cluster, dead_node)

    {:ok, :ok} = App.command(Cluster, {:remove_node, dead_node})

    App.consensus_groups()
    |> Enum.each(fn {group, _n_desired_members} ->
      remove_pid_located_in_dead_node_from_consensus_group(group, dead_node)
    end)
  end

  defunp remove_pid_located_in_dead_node_from_consensus_group(group :: atom, dead_node :: node) ::
           :ok do
    require Logger

    case inspect_statuses_of_consensus_group(group) do
      {:ok, {leader, pairs}} ->
        member_pids_belonging_to_dead_node(pairs, dead_node)
        |> Enum.each(fn dead_pid ->
          ret = RaftNode.remove_follower(leader, dead_pid)

          Logger.info(
            "removed a member #{inspect(dead_pid)} (which resides in #{dead_node}) of group #{group}: #{inspect(ret)}"
          )
        end)

      {:error, pairs} ->
        # There's no leader in this case
        responding_member_pids = Enum.map(pairs, fn {_, s} -> s.from end)

        member_pids_belonging_to_dead_node(pairs, dead_node)
        |> Enum.each(fn dead_pid ->
          Logger.info(
            "forcibly removing a member #{inspect(dead_pid)} (which resides in #{dead_node}) of group #{group}"
          )

          Enum.each(responding_member_pids, fn member_pid ->
            ret = RaftNode.force_remove_member(member_pid, dead_pid)

            Logger.info(
              "made a member #{inspect(member_pid)} forget about #{inspect(dead_pid)}: #{inspect(ret)}"
            )
          end)
        end)
    end
  end

  defp member_pids_belonging_to_dead_node(pairs, dead_node) do
    Enum.flat_map(pairs, fn {_, s} -> s.members end)
    |> Enum.uniq()
    |> Enum.filter(&(node(&1) == dead_node))
  end

  defunp inspect_statuses_of_consensus_group(group :: atom) ::
           {:ok, {pid, [{node, map}]}} | {:error, [{node, map}]} do
    case LeaderPidCache.retrieve_member_statuses(group) do
      [] ->
        {:error, []}

      pairs ->
        {most_supported_leader, count} =
          Enum.reduce(pairs, %{}, fn {_, s}, m ->
            Map.update(m, s.leader, 1, &(&1 + 1))
          end)
          |> Enum.max_by(fn {_, c} -> c end)

        if is_pid(most_supported_leader) and 2 * count > length(pairs) do
          # majority of members agree on that leader
          {:ok, {most_supported_leader, pairs}}
        else
          {:error, pairs}
        end
    end
  end

  defun unreachable_nodes() :: %{node => unreachable_since} when unreachable_since: pos_integer do
    GenServer.call(NodeReconnect, :unreachable_nodes)
  end
end
