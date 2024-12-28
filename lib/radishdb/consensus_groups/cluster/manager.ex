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

  alias RadishDB.ConsensusGroups.GroupApplication
  alias RadishDB.ConsensusGroups.Cluster.{Activator, ConsensusMemberAdjuster, Deactivator}
  alias RadishDB.ConsensusGroups.Config.Config
  alias RadishDB.ConsensusGroups.OTP.{ConsensusMemberSupervisor, ProcessAndDiskLogIndexInspector}
  alias RadishDB.Raft.Node, as: RaftNode
  alias RadishDB.Raft.Types.Config, as: RaftConfig
  alias RadishDB.Raft.Types.Error.AddFollowerError

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
    new_state =
      if State.phase(state) in [:active, :activating] do
        handle_active_state(name, raft_config, member_nodes, state)
      else
        log_inactive_state(name)
        state
      end

    {:noreply, new_state}
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

        if node_to_host_initial_leader == Node.self() do
          start_leader_and_tell_other_nodes_to_start_follower(
            name,
            raft_config,
            member_nodes,
            state
          )
        else
          delegate_leader_to_node(node_to_host_initial_leader, name, raft_config, member_nodes)

          State.update_being_added_consensus_groups(
            state,
            name,
            {:leader_delegated_to, node_to_host_initial_leader}
          )
        end

      {:error, :process_exists} ->
        State.update_being_added_consensus_groups(state, name, :process_exists)
    end
  end

  defp delegate_leader_to_node(node, name, raft_config, member_nodes) do
    start_consensus_group_members({__MODULE__, node}, name, raft_config, member_nodes)
  end

  defp log_inactive_state(name) do
    Logger.info(
      "manager process is not active; cannot start member processes for consensus group #{name}"
    )
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
    case attempt_to_start_follower(other_node_members, name) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} ->
        handle_follower_start_error(reason, name)
        retry_starting_follower(other_node_members, name, tries_remaining)
    end
  end

  defp attempt_to_start_follower(other_node_members, name) do
    :supervisor.start_child(ConsensusMemberSupervisor, [
      {:join_existing_consensus_group, other_node_members},
      name
    ])
  end

  defp handle_follower_start_error(reason, name) do
    case extract_dead_follower_pid_from_reason(reason) do
      nil -> :ok
      dead_follower_pid -> spawn(fn -> try_to_remove_dead_follower(name, dead_follower_pid) end)
    end
  end

  defp retry_starting_follower(other_node_members, name, tries_remaining) do
    if tries_remaining > 0 do
      :timer.sleep(2 * Config.follower_addition_delay())
      start_follower_with_retry(other_node_members, name, tries_remaining - 1)
    else
      {:error, :max_retries_exceeded}
    end
  end

  defp extract_dead_follower_pid_from_reason(%AddFollowerError{pid: pid}), do: pid

  defp extract_dead_follower_pid_from_reason(
         {:timeout, {_mod, _fun, [_server, {:add_follower, pid}, _timeout]}}
       ),
       do: pid

  defp extract_dead_follower_pid_from_reason(_), do: nil

  defp try_to_remove_dead_follower(name, dead_follower_pid) do
    case GroupApplication.whereis_leader(name) do
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
