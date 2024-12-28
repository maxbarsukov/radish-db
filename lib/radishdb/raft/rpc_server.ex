use Croma

defmodule RadishDB.Raft.RPCServer do
  @moduledoc """
  A module that implements the Raft RPC server for managing the consensus protocol.

  The `RPCServer` module handles the communication between Raft nodes,
  processing both asynchronous and synchronous messages.
  It manages state transitions for leader, candidate, and follower nodes,
  ensuring that the Raft consensus algorithm is correctly implemented.

  ## Events

  - async (member-to-member messages)
    - defined as Raft RPC (all contain `term`)
      - AppendEntriesRequest
      - AppendEntriesResponse
      - RequestVoteRequest
      - RequestVoteResponse
      - InstallSnapshot, InstallSnapshotCompressed
      - TimeoutNow
    - others
      - :heartbeat_timeout
      - :election_timeout
      - :cannot_reach_quorum
      - :remove_follower_completed
      - {:DOWN, _, :process, _, _} # temporary snapshot writer process terminated
  - sync
    - defined in Raft (client-to-leader messages)
      - {:command, arg, cmd_id}
      - {:query, arg}
      - {:change_config, new_config}
      - {:add_follower, pid}
      - {:remove_follower, pid}
      - {:replace_leader, new_leader}
    - others (client-to-anymember messages)
      - {:query_non_leader, arg}
      - {:snapshot_created, path, term, index, size}
      - {:force_remove_member, pid}
      - :status

  ## State transitions

  - :leader or :candidate => :follower, when newer term started
    - in this case the incoming message that triggers the transition should be handled as a follower
    - implemented in `become_follower_if_new_term_started`
  - :follower => :candidate, when election_timeout elapses
    - implemented in `follower(:election_timeout, state)`
  - :candidate => :follower, when new leader found
    - in this case the incoming message that triggers the transition should be handled as a follower
    - implemented in `handle_append_entries_request`
  - :candidate => :leader, when majority agrees
    - implemented in `candidate(%RequestVoteResponse{}, state)` (and in `become_candidate_and_start_new_election/2` for a special case)
  - :leader => :follower, when stepping down to replace leader
    - implemented in `leader(%AppendEntriesResponse{}, state)`
  - :leader => :follower, when election timeout elapses without getting responses from majority
    - implemented in `leader(:cannot_reach_quorum, state)`

  ## Miscellaneous Notes

  - To make command execution "linearizable":
    (note that this is basically equivalent to implicitly establish client session for each command)
      1. client assigns a unique ID to each command
      2. servers cache responses of command executions
      3. if a cached response is found for a command, don't execute the command twice and just returns the cached response
  """

  alias RadishDB.Raft.Communication.{Election, Leadership, Members}
  alias RadishDB.Raft.Log.Entry
  alias RadishDB.Raft.Persistence.{Logs, Snapshot, SnapshotMetadata, Store}
  alias RadishDB.Raft.Types.{CommandResults, Config, TermNumber}
  alias RadishDB.Raft.Types.Error.AddFollowerError

  alias RadishDB.Raft.Types.RPC.{
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshot,
    InstallSnapshotCompressed,
    RequestVoteRequest,
    RequestVoteResponse,
    TimeoutNow
  }

  alias RadishDB.Raft.Utils.Collections.PidSet
  alias RadishDB.Raft.Utils.Monotonic

  defmodule State do
    @moduledoc """
    State of Raft Server.
    """

    use Croma.Struct,
      fields: [
        members: Members,
        current_term: TermNumber,
        leadership: Croma.TypeGen.nilable(Leadership),
        election: Election,
        logs: Logs,
        # replicated using raft logs (i.e. reproducible from logs)
        data: Croma.Any,
        # replicated using raft logs (i.e. reproducible from logs)
        command_results: CommandResults,
        config: Config,
        store: Croma.TypeGen.nilable(Store)
      ]
  end

  @behaviour :gen_statem
  def callback_mode, do: :state_functions

  defp next_state(state_data, state_name) do
    {:next_state, state_name, state_data}
  end

  defp keep_fsm_state(state_data) do
    {:keep_state, state_data}
  end

  defp keep_fsm_state_and_reply(state_data, from, reply) do
    reply(state_data, from, reply)
    {:keep_state, state_data}
  end

  ### Initialization ###

  def init({{:create_new_consensus_group, config}, options}) do
    {:ok, :leader, initialize_leader_state(config, options)}
  end

  def init({{:join_existing_consensus_group, known_members}, options}) do
    {:ok, :follower, initialize_follower_state(known_members, options)}
  end

  defunp initialize_leader_state(config :: Config.t(), options :: [RadishDB.Raft.Node.option()]) ::
           State.t() do
    case Keyword.get(options, :persistence_dir) do
      nil ->
        snapshot = generate_empty_snapshot_for_lonely_leader(config)
        logs = Logs.new_for_lonely_leader(snapshot.last_committed_entry, [])
        build_state_from_snapshot(snapshot, logs, nil)

      dir ->
        log_expansion_factor = Keyword.fetch!(options, :log_file_expansion_factor)

        case Snapshot.read_latest_snapshot_and_logs_if_available(dir) do
          nil ->
            snapshot = generate_empty_snapshot_for_lonely_leader(config)
            logs = Logs.new_for_lonely_leader(snapshot.last_committed_entry, [])

            store =
              Store.new_with_initial_snapshotting(dir, log_expansion_factor, snapshot)

            build_state_from_snapshot(snapshot, logs, store)

          tuple ->
            recover_state_from_snapshot_and_log(dir, tuple, log_expansion_factor)
        end
    end
  end

  defunp generate_empty_snapshot_for_lonely_leader(config :: Config.t()) :: Snapshot.t() do
    %Snapshot{
      members: Members.new_for_lonely_leader(),
      term: 0,
      last_committed_entry: {0, 1, :leader_elected, [self()]},
      data: config.data_module.new(),
      command_results: CommandResults.new(),
      config: config
    }
  end

  defunp build_state_from_snapshot(
           %Snapshot{
             members: members,
             term: term,
             data: data,
             command_results: command_results,
             config: config
           },
           logs :: Logs.t(),
           store :: nil | Store.t()
         ) :: State.t() do
    %State{
      members: members,
      current_term: term,
      leadership: Leadership.new_for_leader(config),
      election: Election.new_for_leader(),
      logs: logs,
      data: data,
      command_results: command_results,
      config: config,
      store: store
    }
  end

  defp recover_state_from_snapshot_and_log(
         dir,
         {snapshot_from_disk, snapshot_meta, log_entries},
         log_expansion_factor
       ) do
    # In this case we neglect `config` given in the argument to `RadishDB.Raft.Node.start_link/2`.
    # On the other hand we discard `members` obtained from disk,
    # as `self()` is the sole member of this newly-spawned consensus group.
    members = Members.new_for_lonely_leader()
    snapshot = %Snapshot{snapshot_from_disk | members: members}
    logs1 = Logs.new_for_lonely_leader(snapshot.last_committed_entry, log_entries)
    {logs2, entry_restore} = Logs.add_entry_on_restored_from_files(logs1, snapshot.term)

    store =
      Store.new_with_disk_snapshot(dir, log_expansion_factor, snapshot_meta, entry_restore)

    {logs3, entries_to_apply} = Logs.commit_to_latest(logs2, store)
    state1 = build_state_from_snapshot(snapshot, logs3, store)
    Process.put(:radish_db_raft_rpc_server_restoring, true)
    # `entry_restore` results in a no-op and thus neglected
    state2 =
      Enum.reduce(
        entries_to_apply,
        state1,
        &leader_apply_committed_log_entry_without_membership_change/2
      )

    Process.delete(:radish_db_raft_rpc_server_restoring)
    run_restored_hook(state2)
    state2
  end

  defp run_restored_hook(%State{config: %Config{leader_hook_module: hook}, data: data}) do
    case hook do
      nil -> :ok
      _ -> hook.on_restored_from_files(data)
    end
  end

  defunp initialize_follower_state(
           known_members :: [GenServer.server()],
           options :: [RadishDB.Raft.Node.option()]
         ) :: State.t() do
    %Snapshot{
      members: members,
      term: term,
      last_committed_entry: last_entry,
      data: data,
      command_results: command_results,
      config: config
    } =
      snapshot =
      call_add_server_with_fault_injection(known_members, options)

    logs = Logs.new_for_new_follower(last_entry)
    election = Election.new_for_follower(config)

    store =
      case Keyword.get(options, :persistence_dir) do
        nil ->
          nil

        dir ->
          log_expansion_factor = Keyword.fetch!(options, :log_file_expansion_factor)
          Store.new_with_snapshot_sent_from_leader(dir, log_expansion_factor, snapshot)
      end

    %State{
      members: members,
      current_term: term,
      election: election,
      logs: logs,
      data: data,
      command_results: command_results,
      config: config,
      store: store
    }
  end

  defunp call_add_server_with_fault_injection(
           known_members :: [GenServer.server()],
           options :: [RadishDB.Raft.Node.option()]
         ) :: Snapshot.t() do
    snapshot = call_add_server_and_reraise_with_pid(known_members)
    # The following is solely for testing
    case Keyword.get(options, :test_inject_fault_after_add_follower) do
      :raise ->
        raise AddFollowerError,
          message:
            "simulated error in RadishDB.Raft.RPCServer.call_add_server_with_fault_injection/2",
          pid: self()

      :timeout ->
        exit(
          {:timeout,
           {:gen_statem, :call,
            [Enum.random(known_members), {:add_follower, self()}, {:dirty_timeout, 5000}]}}
        )

      nil ->
        snapshot
    end
  end

  defunp call_add_server_and_reraise_with_pid(known_members :: [GenServer.server()]) ::
           Snapshot.t() do
    try do
      call_add_server(known_members)
    rescue
      e ->
        # Pass `self()` to caller of `Supervisor.start_child/2` for cleanup of consensus group
        reraise(AddFollowerError, [message: Exception.message(e), pid: self()], __STACKTRACE__)
    end
  end

  defunp call_add_server(known_members :: [GenServer.server()]) :: Snapshot.t() do
    [] ->
      raise "no leader found"

    [m | ms] ->
      case call_add_server_one(m) do
        {:ok, %InstallSnapshot{} = is} ->
          Snapshot.from_install_snapshot(is)

        {:ok, %InstallSnapshotCompressed{bin: bin}} ->
          Snapshot.decode(bin)

        {:error, {:not_leader, nil}} ->
          call_add_server(ms)

        {:error, {:not_leader, leader}} ->
          call_add_server([leader | List.delete(ms, leader)])

        {:error, :noproc} ->
          call_add_server(ms)
          # {:error, :uncommitted_membership_change} results in an error
      end
  end

  defp call_add_server_one(maybe_leader) do
    :gen_statem.call(maybe_leader, {:add_follower, self()}, {:dirty_timeout, 5000})
  catch
    :exit, {:noproc, _} -> {:error, :noproc}
  end

  ### Leader state ###

  defp handle_success_response(
         new_leadership,
         state,
         logs,
         members,
         current_term,
         from,
         i_replicated,
         store
       ) do
    {new_logs, entries_to_apply} =
      Logs.set_follower_index(logs, members, current_term, from, i_replicated, store)

    new_state1 = %State{state | leadership: new_leadership, logs: new_logs}

    new_state2 =
      Enum.reduce(entries_to_apply, new_state1, &leader_apply_committed_log_entry/2)

    case members do
      %Members{pending_leader_change: ^from} ->
        # now we know that the follower `from` is alive => make it a new leader
        case Logs.make_append_entries_req(new_logs, current_term, from, Monotonic.milliseconds()) do
          {:ok, append_req} ->
            req = %TimeoutNow{append_entries_req: append_req}
            cast(new_state2, from, req)
            # step down in order not to serve client requests any more
            convert_state_as_follower(new_state2, current_term) |> next_state(:follower)

          {:too_old, _} ->
            # `from`'s logs lag too behind => try leader change next time
            keep_fsm_state(new_state2)
        end

      _ ->
        keep_fsm_state(new_state2)
    end
  end

  def leader(
        :cast,
        %AppendEntriesResponse{
          from: from,
          success: success,
          i_replicated: i_replicated,
          leader_timestamp: leader_timestamp
        } = rpc,
        %State{
          members: members,
          current_term: current_term,
          leadership: leadership,
          logs: logs,
          config: config,
          store: store
        } = state
      ) do
    become_follower_if_new_term_started(rpc, state, fn ->
      new_leadership =
        Leadership.follower_responded(leadership, members, from, leader_timestamp, config)

      if success do
        handle_success_response(
          new_leadership,
          state,
          logs,
          members,
          current_term,
          from,
          i_replicated,
          store
        )
      else
        # prev log from leader didn't match follower's => decrement "next index"
        # for the follower and try to resend AppendEntries
        new_logs = Logs.decrement_next_index_of_follower(logs, from)

        %State{state | leadership: new_leadership, logs: new_logs}
        |> send_append_entries(from, Monotonic.milliseconds())
        |> keep_fsm_state()
      end
    end)
  end

  def leader(:cast, :heartbeat_timeout, state) do
    broadcast_append_entries(state) |> keep_fsm_state()
  end

  def leader(:cast, :cannot_reach_quorum, %State{current_term: term} = state) do
    convert_state_as_follower(state, term)
    |> next_state(:follower)
  end

  def leader(:cast, %RequestVoteRequest{} = rpc, state) do
    handle_request_vote_request(rpc, state, :leader)
  end

  def leader(:cast, %s{} = rpc, state) when s in [AppendEntriesRequest, RequestVoteResponse] do
    become_follower_if_new_term_started(rpc, state, fn ->
      # neglect `AppendEntriesRequest`, `RequestVoteResponse` for this term / older term
      keep_fsm_state(state)
    end)
  end

  def leader(:cast, msg, state) do
    # leader neglects `:election_timeout`, `:remove_follower_completed`, `InstallSnapshot`, `TimeoutNow`
    handle_cast_common(msg, :leader, state)
  end

  def leader(
        {:call, from},
        {:command, arg, cmd_id},
        %State{current_term: term, logs: logs, store: store} = state
      ) do
    {new_logs, entry} =
      Logs.add_entry(logs, store, fn index ->
        {term, index, :command, {from, arg, cmd_id}}
      end)

    %State{state | logs: new_logs}
    |> persist_log_entries([entry])
    |> broadcast_append_entries()
    |> keep_fsm_state()
  end

  def leader(
        {:call, from},
        {:query, arg},
        %State{
          members: members,
          current_term: term,
          leadership: leadership,
          logs: logs,
          config: config,
          store: store
        } = state
      ) do
    if Leadership.lease_expired?(leadership, members, config) do
      # if leader's lease has already expired, fall back to log replication (handled in the same way as commands)
      {new_logs, entry} =
        Logs.add_entry(logs, store, fn index -> {term, index, :query, {from, arg}} end)

      %State{state | logs: new_logs}
      |> persist_log_entries([entry])
      |> broadcast_append_entries()
      |> keep_fsm_state()
    else
      # with valid lease, leader can respond by itself
      run_query(state, {from, arg})
      keep_fsm_state(state)
    end
  end

  def leader(
        {:call, from},
        {:change_config, new_config},
        %State{current_term: term, logs: logs, store: store} = state
      ) do
    {new_logs, entry} =
      Logs.add_entry(logs, store, fn index -> {term, index, :change_config, new_config} end)

    %State{state | logs: new_logs}
    |> persist_log_entries([entry])
    |> keep_fsm_state_and_reply(from, :ok)
  end

  def leader(
        {:call, from},
        {:add_follower, new_follower},
        %State{members: members, current_term: term, logs: logs, store: store} = state
      ) do
    {new_logs, add_follower_entry} =
      Logs.add_entry_on_add_follower(logs, term, new_follower, store)

    case Members.start_adding_follower(members, add_follower_entry) do
      {:error, _} = e ->
        # we have to revert to the `state` without `add_follower_entry`
        keep_fsm_state_and_reply(state, from, e)

      {:ok, new_members} ->
        %State{state | members: new_members, logs: new_logs}
        |> persist_log_entries([add_follower_entry])
        |> send_snapshot(new_follower, fn mod, snapshot -> mod.reply(from, {:ok, snapshot}) end)
        |> broadcast_append_entries()
        |> keep_fsm_state()
    end
  end

  def leader(
        {:call, from},
        {:remove_follower, old_follower},
        %State{
          members: members,
          current_term: term,
          leadership: leadership,
          logs: logs,
          config: config,
          store: store
        } = state
      ) do
    {new_logs, remove_follower_entry} =
      Logs.add_entry_on_remove_follower(logs, term, old_follower, store)

    case Members.start_removing_follower(members, remove_follower_entry) do
      {:error, _} = e ->
        # we have to revert to the `state` without `remove_follower_entry`
        keep_fsm_state_and_reply(state, from, e)

      {:ok, new_members} ->
        if Leadership.can_safely_remove?(leadership, members, old_follower, config) do
          new_leadership =
            Leadership.remove_follower_response_time_entry(leadership, old_follower)

          %State{state | members: new_members, leadership: new_leadership, logs: new_logs}
          |> persist_log_entries([remove_follower_entry])
          |> broadcast_append_entries()
          |> keep_fsm_state_and_reply(from, :ok)
        else
          # we have to revert to the `state` without `remove_follower_entry`
          keep_fsm_state_and_reply(state, from, {:error, :will_break_quorum})
        end
    end
  end

  def leader(
        {:call, from},
        {:replace_leader, new_leader},
        %State{members: members, leadership: leadership, config: config} = state
      ) do
    # We don't immediately try to replace leader;
    # instead we invoke replacement when receiving message from the target member
    case Members.start_replacing_leader(members, new_leader) do
      {:error, _} = e ->
        keep_fsm_state_and_reply(state, from, e)

      {:ok, new_members} ->
        if new_leader in Leadership.unresponsive_followers(leadership, members, config) do
          keep_fsm_state_and_reply(state, from, {:error, :new_leader_unresponsive})
        else
          %State{state | members: new_members} |> keep_fsm_state_and_reply(from, :ok)
        end
    end
  end

  def leader({:call, from}, msg, state) do
    handle_call_common(msg, from, :leader, state)
  end

  def leader(:info, msg, state) do
    handle_info_common(msg, :leader, state)
  end

  def become_leader(
        %State{
          members: members,
          current_term: term,
          logs: logs,
          config: config,
          store: store
        } = state
      ) do
    leadership = Leadership.new_for_leader(config)
    {new_logs, entry} = Logs.add_entry_on_elected_leader(logs, members, term, store)

    %State{
      state
      | members: Members.put_leader(members, self()),
        leadership: leadership,
        logs: new_logs
    }
    |> persist_log_entries([entry])
    |> broadcast_append_entries()
    |> next_state(:leader)
  end

  defunp broadcast_append_entries(
           %State{
             members: members,
             leadership: leadership,
             logs: logs,
             config: config,
             store: store
           } = state
         ) :: State.t() do
    followers = Members.other_members_list(members)

    if Enum.empty?(followers) do
      # When there's no other member in this consensus group, the leader won't receive AppendEntriesResponse;
      # here is the time to make decisions (solely by itself) by committing new entries.
      {new_logs, entries_to_apply} = Logs.commit_to_latest(logs, store)
      # quorum is reached by the leader itself
      new_leadership = Leadership.reset_quorum_timer(leadership, config)
      new_state = %State{state | leadership: new_leadership, logs: new_logs}
      Enum.reduce(entries_to_apply, new_state, &leader_apply_committed_log_entry/2)
    else
      now = Monotonic.milliseconds()

      Enum.reduce(followers, state, fn follower, s ->
        send_append_entries(s, follower, now)
      end)
    end
    |> reset_heartbeat_timer()
  end

  defunp send_append_entries(
           %State{current_term: term, logs: logs} = state,
           follower :: pid,
           now :: Monotonic.t()
         ) :: State.t() do
    case Logs.make_append_entries_req(logs, term, follower, now) do
      {:ok, req} ->
        cast(state, follower, req)
        state

      {:too_old, new_logs} ->
        %State{state | logs: new_logs}
        |> send_snapshot(follower, fn mod, snapshot -> mod.cast(follower, snapshot) end)

      :error ->
        # `follower` is not included in `logs`; this indicates that `follower` is already removed => neglect
        state
    end
  end

  ### Candidate state ###

  def candidate(:cast, %AppendEntriesRequest{} = req, state) do
    handle_append_entries_request(req, state)
  end

  def candidate(:cast, %AppendEntriesResponse{} = rpc, state) do
    become_follower_if_new_term_started(rpc, state, fn ->
      # neglect `AppendEntriesResponse` from this term / older term
      keep_fsm_state(state)
    end)
  end

  def candidate(:cast, %RequestVoteRequest{} = rpc, state) do
    handle_request_vote_request(rpc, state, :candidate)
  end

  def candidate(
        :cast,
        %RequestVoteResponse{from: from, term: term, vote_granted: granted?} = rpc,
        %State{members: members, current_term: current_term, election: election} = state
      ) do
    become_follower_if_new_term_started(rpc, state, fn ->
      if term < current_term or !granted? do
        # neglect `RequestVoteResponse` from older term
        keep_fsm_state(state)
      else
        {new_election, majority?} = Election.gain_vote(election, members, from)
        new_state = %State{state | election: new_election}

        choose_candidate_action(majority?, new_state)
      end
    end)
  end

  def candidate(:cast, :election_timeout, state) do
    become_candidate_and_start_new_election(state)
  end

  def candidate(:cast, msg, state) do
    # neglect `:heartbeat_timeout`, `:remove_follower_completed`, `cannot_reach_quorum`, `InstallSnapshot`, `TimeoutNow`
    handle_cast_common(msg, :candidate, state)
  end

  def candidate({:call, from}, msg, state) do
    # non-leader rejects synchronous events:
    # - `{:command, arg, cmd_id}`,
    # - `{:query, arg}`,
    # - `{:change_config, new_config}`,
    # - `{:add_follower, pid}`,
    # - `{:remove_follower, pid}`,
    # - `{:replace_leader, new_leader}`
    handle_call_common(msg, from, :candidate, state)
  end

  def candidate(:info, msg, state) do
    handle_info_common(msg, :candidate, state)
  end

  defp choose_candidate_action(majority?, new_state) do
    if majority? do
      become_leader(new_state)
    else
      keep_fsm_state(new_state)
    end
  end

  defp become_candidate_and_start_new_election(
         %State{members: members, current_term: term, election: election, config: config} = state,
         replacing_leader? \\ false
       ) do
    members_set = members.all

    if PidSet.size(members_set) == 1 and PidSet.member?(members_set, self()) do
      # 1-member consensus group must be handled separately.
      # As `self()` already has vote from majority (i.e. itself), no election is needed;
      # skip candidate state and directly become a leader.
      %State{state | current_term: term + 1, election: Election.new_for_leader()}
      |> become_leader()
    else
      new_members = Members.put_leader(members, nil)
      new_election = Election.update_for_candidate(election, config)

      new_state = %State{
        state
        | members: new_members,
          current_term: term + 1,
          election: new_election
      }

      broadcast_request_vote(new_state, replacing_leader?)
      next_state(new_state, :candidate)
    end
  end

  defunp broadcast_request_vote(
           %State{members: members, current_term: term, logs: logs} = state,
           replacing_leader? :: boolean
         ) :: :ok do
    Members.other_members_list(members)
    |> Enum.each(fn member ->
      {last_log_term, last_log_index, _, _} = Logs.last_entry(logs)

      req = %RequestVoteRequest{
        term: term,
        candidate_pid: self(),
        last_log: {last_log_term, last_log_index},
        replacing_leader: replacing_leader?
      }

      cast(state, member, req)
    end)
  end

  #
  # follower state
  #
  def follower(:cast, %AppendEntriesRequest{} = req, state) do
    handle_append_entries_request(req, state)
  end

  def follower(:cast, %RequestVoteRequest{} = rpc, state) do
    handle_request_vote_request(rpc, state, :follower)
  end

  def follower(:cast, %s{} = rpc, state) when s in [AppendEntriesResponse, RequestVoteResponse] do
    become_follower_if_new_term_started(rpc, state, fn ->
      # neglect `AppendEntriesResponse`, `RequestVoteResponse` from this term / older term
      keep_fsm_state(state)
    end)
  end

  def follower(:cast, :election_timeout, state) do
    become_candidate_and_start_new_election(state)
  end

  def follower(
        :cast,
        %TimeoutNow{append_entries_req: req},
        %State{members: members, current_term: current_term, logs: logs, store: store} =
          state
      ) do
    %AppendEntriesRequest{
      term: term,
      prev_log: prev_log,
      entries: entries,
      i_leader_commit: i_leader_commit
    } = req

    if term == current_term and Logs.contain_given_prev_log?(logs, prev_log) do
      # catch up with the leader and then start election
      {new_logs, new_members1, entries_to_apply, entries_to_persist} =
        Logs.append_entries(logs, members, entries, i_leader_commit, store)

      new_state = %State{state | members: new_members1, logs: new_logs}

      Enum.reduce(entries_to_apply, new_state, &nonleader_apply_committed_log_entry/2)
      |> persist_log_entries(entries_to_persist)
      |> become_candidate_and_start_new_election(true)
    else
      # if condition is not met neglect the message
      keep_fsm_state(state)
    end
  end

  def follower(:cast, :remove_follower_completed, state) do
    {:stop, :normal, state}
  end

  def follower(:cast, %InstallSnapshot{} = rpc, state) do
    handle_install_snapshot(Snapshot.from_install_snapshot(rpc), state)
    |> keep_fsm_state()
  end

  def follower(:cast, %InstallSnapshotCompressed{bin: bin}, state) do
    handle_install_snapshot(Snapshot.decode(bin), state)
    |> keep_fsm_state()
  end

  def follower(:cast, msg, state) do
    # neglect `:heartbeat_timeout`, `cannot_reach_quorum`
    handle_cast_common(msg, :follower, state)
  end

  def follower({:call, from}, msg, state) do
    # non-leader rejects synchronous events:
    # - `{:command, arg, cmd_id}`,
    # - `{:query, arg}`, `{:change_config, new_config}`,
    # - `{:add_follower, pid}`,
    # - `{:remove_follower, pid}`,
    # - `{:replace_leader, new_leader}`
    handle_call_common(msg, from, :follower, state)
  end

  def follower(:info, msg, state) do
    handle_info_common(msg, :follower, state)
  end

  defp become_follower_if_new_term_started(
         %{term: term} = rpc,
         %State{current_term: current_term} = state,
         else_fn
       ) do
    if term > current_term do
      new_state = convert_state_as_follower(state, term)
      # process the given RPC message as a follower using :next_event action
      # (there are cases where `election.timer` started right above will be immediately resetted in `follower/2` but it's rare)
      {:next_state, :follower, new_state, [{:next_event, :cast, rpc}]}
    else
      else_fn.()
    end
  end

  defunp convert_state_as_follower(
           %State{members: members, leadership: leadership, election: election, config: config} =
             state,
           new_term :: TermNumber.t()
         ) :: State.t() do
    if leadership, do: Leadership.stop_timers(leadership)
    new_members = Members.put_leader(members, nil)
    new_election = Election.update_for_follower(election, config)

    %State{
      state
      | members: new_members,
        current_term: new_term,
        leadership: nil,
        election: new_election
    }
  end

  defunp handle_install_snapshot(
           %Snapshot{
             members: members,
             term: term,
             last_committed_entry: last_entry,
             data: data,
             command_results: command_results
           } = snapshot,
           %State{store: store} = state
         ) :: State.t() do
    become_follower_if_new_term_started(snapshot, state, fn ->
      new_logs = Logs.new_for_new_follower(last_entry)
      # invalidate existing snapshot file
      new_store =
        if is_nil(store), do: nil, else: Store.unset_snapshot_metadata(store)

      %State{
        state
        | members: members,
          current_term: term,
          logs: new_logs,
          data: data,
          command_results: command_results,
          store: new_store
      }
      |> reset_election_timer_on_leader_message()
    end)
  end

  ### Common handler implementations

  defp handle_append_entries_request(
         %AppendEntriesRequest{
           term: term,
           leader_pid: leader_pid,
           prev_log: prev_log,
           entries: entries,
           i_leader_commit: i_leader_commit,
           leader_timestamp: leader_timestamp
         },
         %State{
           members: members,
           current_term: current_term,
           logs: logs,
           store: store
         } = state
       ) do
    reply_as_failure = fn larger_term ->
      cast(state, leader_pid, %AppendEntriesResponse{
        from: self(),
        term: larger_term,
        i_replicated: nil,
        success: false,
        leader_timestamp: leader_timestamp
      })
    end

    if term < current_term do
      # AppendEntries from leader for older term => reject
      reply_as_failure.(current_term)
      keep_fsm_state(state)
    else
      if Logs.contain_given_prev_log?(logs, prev_log) do
        {new_logs, new_members1, entries_to_apply, entries_to_persist} =
          Logs.append_entries(logs, members, entries, i_leader_commit, store)

        new_members2 = Members.put_leader(new_members1, leader_pid)
        new_state1 = %State{state | members: new_members2, current_term: term, logs: new_logs}

        new_state2 =
          Enum.reduce(entries_to_apply, new_state1, &nonleader_apply_committed_log_entry/2)
          |> persist_log_entries(entries_to_persist)

        reply = %AppendEntriesResponse{
          from: self(),
          term: term,
          success: true,
          i_replicated: new_logs.i_max,
          leader_timestamp: leader_timestamp
        }

        cast(new_state2, leader_pid, reply)
        new_state2
      else
        # this follower does not have `prev_log` => ask leader to resend older logs
        reply_as_failure.(term)
        new_members = Members.put_leader(members, leader_pid)
        %State{state | members: new_members, current_term: term}
      end
      |> reset_election_timer_on_leader_message()
      |> next_state(:follower)
    end
  end

  defp handle_request_vote_request(
         %RequestVoteRequest{
           candidate_pid: candidate,
           replacing_leader: replacing?
         } = rpc,
         %State{current_term: current_term} =
           state,
         current_state_name
       ) do
    if replacing? or leader_authority_timed_out?(current_state_name, state) do
      become_follower_if_new_term_started(rpc, state, fn ->
        handle_vote_request(rpc, state, current_state_name)
      end)
    else
      # Reject vote request if leader lease has not yet expired
      reject_vote_request(candidate, current_term, state, current_state_name)
    end
  end

  defp handle_vote_request(
         %RequestVoteRequest{
           term: term,
           candidate_pid: candidate,
           last_log: last_log
         },
         %State{current_term: current_term, election: election, logs: logs, config: config} =
           state,
         current_state_name
       ) do
    grant_vote? = can_grant_vote?(term, candidate, current_term, election, logs, last_log)

    response = %RequestVoteResponse{
      from: self(),
      term: current_term,
      vote_granted: grant_vote?
    }

    cast(state, candidate, response)

    new_state =
      if grant_vote? do
        %State{state | election: Election.vote_for(election, candidate, config)}
      else
        state
      end

    next_state(new_state, current_state_name)
  end

  defp can_grant_vote?(term, candidate, current_term, election, logs, last_log) do
    # the case `term > current_term` is covered by `become_follower_if_new_term_started`
    term == current_term and
      election.voted_for in [nil, candidate] and
      Logs.candidate_log_up_to_date?(logs, last_log)
  end

  defp reject_vote_request(candidate, current_term, state, current_state_name) do
    response = %RequestVoteResponse{from: self(), term: current_term, vote_granted: false}
    cast(state, candidate, response)
    next_state(state, current_state_name)
  end

  defp handle_cast_common(_event, _state_name, state) do
    keep_fsm_state(state)
  end

  defp handle_call_common({:query_non_leader, arg}, from, _state_name, state) do
    run_query_without_leader_hook(state, from, arg)
    keep_fsm_state(state)
  end

  defp handle_call_common(
         {:snapshot_created, path, term, index, size},
         from,
         _state_name,
         %State{store: store} = state
       ) do
    snapshot_meta = %SnapshotMetadata{
      path: path,
      term: term,
      last_committed_index: index,
      size: size
    }

    new_store = %Store{store | latest_snapshot_metadata: snapshot_meta}
    new_state = %State{state | store: new_store}
    keep_fsm_state_and_reply(new_state, from, :ok)
  end

  defp handle_call_common(
         {:force_remove_member, member_to_remove},
         from,
         _state_name,
         %State{members: members} = state
       ) do
    # There are cases where removing a member can trigger state transition (e.g. candidate => leader).
    # To make things simpler, we defer those state transitions to next timer event (e.g. next election timeout).
    new_members = Members.force_remove_member(members, member_to_remove)
    new_state = %State{state | members: new_members}
    keep_fsm_state_and_reply(new_state, from, :ok)
  end

  defp handle_call_common(
         :status,
         from,
         state_name,
         %State{
           members: members,
           current_term: current_term,
           leadership: leadership,
           config: config
         } = state
       ) do
    unresponsive_followers =
      case state_name do
        :leader -> Leadership.unresponsive_followers(leadership, members, config)
        _ -> []
      end

    reply = %{
      from: self(),
      members: PidSet.to_list(members.all),
      leader: members.leader,
      unresponsive_followers: unresponsive_followers,
      current_term: current_term,
      state_name: state_name,
      config: config
    }

    keep_fsm_state_and_reply(state, from, reply)
  end

  defp handle_call_common(_event, from, state_name, state) do
    reason =
      case state_name do
        :leader -> :unexpected_message
        _ -> {:not_leader, state.members.leader}
      end

    keep_fsm_state_and_reply(state, from, {:error, reason})
  end

  def handle_info_common(
        {:DOWN, _ref, :process, _pid, _reason},
        state_name,
        %State{store: store} = state
      ) do
    %State{state | store: %Store{store | snapshot_writer: nil}}
    |> next_state(state_name)
  end

  def handle_info_common(_msg, state_name, state) do
    next_state(state, state_name)
  end

  def terminate(_reason, _state_name, _state) do
    :ok
  end

  def code_change(_old, state_name, state, _extra) do
    {:ok, state_name, state}
  end

  #
  # utilities (misc)
  #
  defp cast(%State{config: %Config{communication_module: mod}}, dest, event) do
    mod.cast(dest, event)
  end

  defp reply(%State{config: %Config{communication_module: mod}}, from, reply) do
    mod.reply(from, reply)
  end

  defunp reset_heartbeat_timer(%State{leadership: leadership, config: config} = state) ::
           State.t() do
    %State{state | leadership: Leadership.reset_heartbeat_timer(leadership, config)}
  end

  defunp reset_election_timer_on_leader_message(
           %State{election: election, config: config} = state
         ) :: State.t() do
    %State{state | election: Election.reset_timer(election, config)}
  end

  defunp leader_authority_timed_out?(current_state_name :: atom, state :: State.t()) :: boolean do
    :leader, %State{members: members, leadership: leadership, config: config} ->
      Leadership.lease_expired?(leadership, members, config)

    _, %State{election: election, config: config} ->
      Election.minimum_timeout_elapsed_since_last_leader_message?(election, config)
  end

  #
  # utilities (command and query)
  #
  defunp leader_apply_committed_log_entry(
           entry :: Entry.t(),
           %State{members: members, data: data, config: %Config{leader_hook_module: hook}} = state
         ) :: State.t() do
    case entry do
      {_term, _index, :command, tuple} ->
        run_command(state, tuple, true)

      {_term, _index, :query, tuple} ->
        run_query(state, tuple)
        state

      {_term, _index, :change_config, new_config} ->
        %State{state | config: new_config}

      {_term, _index, :leader_elected, [leader_pid | _follower_pids]} ->
        if leader_pid == self(), do: hook.on_elected(data)
        state

      {_term, index, :add_follower, follower_pid} ->
        hook.on_follower_added(data, follower_pid)
        %State{state | members: Members.membership_change_committed(members, index)}

      {_term, index, :remove_follower, follower_pid} ->
        # don't use :gen_statem.stop in order to stop `follower_pid` only when it's actually a follower
        cast(state, follower_pid, :remove_follower_completed)
        hook.on_follower_removed(data, follower_pid)
        %State{state | members: Members.membership_change_committed(members, index)}

      {_term, _index, :restore_from_files, _leader_pid} ->
        state
    end
  end

  defunp leader_apply_committed_log_entry_without_membership_change(
           entry :: Entry.t(),
           %State{} = state
         ) :: State.t() do
    # leader is recovering from snapshot & log in disk;
    # there's currently no other member and thus membership-change-related log entries are neglected here
    case entry do
      {_term, _index, :command, tuple} ->
        run_command(state, tuple, true)

      {_term, _index, :query, tuple} ->
        run_query(state, tuple)
        state

      {_term, _index, :change_config, new_config} ->
        %State{state | config: new_config}

      {_term, _index, _otherwise, _} ->
        state
    end
  end

  defunp nonleader_apply_committed_log_entry(entry :: Entry.t(), %State{members: members} = state) ::
           State.t() do
    case entry do
      {_term, _index, :command, tuple} ->
        run_command(state, tuple, false)

      {_term, _index, :change_config, new_config} ->
        %State{state | config: new_config}

      {_term, index, :add_follower, _follower_pid} ->
        %State{state | members: Members.membership_change_committed(members, index)}

      {_term, index, :remove_follower, _follower_pid} ->
        %State{state | members: Members.membership_change_committed(members, index)}

      {_term, _index, _otherwise, _} ->
        state
    end
  end

  defp run_command(
         %State{data: data, command_results: command_results, config: config} = state,
         {client, arg, cmd_id},
         leader?
       ) do
    case CommandResults.fetch(command_results, cmd_id) do
      {:ok, result} ->
        # this command is already executed => don't execute command twice and just return
        if leader?, do: reply(state, client, {:ok, result})
        state

      :error ->
        %Config{data_module: mod, leader_hook_module: hook, max_retained_command_results: max} =
          config

        {result, new_data} = mod.command(data, arg)
        new_command_results = CommandResults.put(command_results, cmd_id, result, max)

        if leader? do
          reply(state, client, {:ok, result})
          hook.on_command_committed(data, arg, result, new_data)
        end

        %State{state | data: new_data, command_results: new_command_results}
    end
  end

  defp run_query(
         %State{data: data, config: %Config{data_module: mod, leader_hook_module: hook}} = state,
         {client, arg}
       ) do
    ret = mod.query(data, arg)
    reply(state, client, {:ok, ret})
    hook.on_query_answered(data, arg, ret)
  end

  defp run_query_without_leader_hook(
         %State{data: data, config: %Config{data_module: mod}} = state,
         client,
         arg
       ) do
    ret = mod.query(data, arg)
    reply(state, client, {:ok, ret})
  end

  #
  # utilities (persisting logs and snapshots)
  #
  defunp persist_log_entries(
           %State{logs: logs, store: store0} = state,
           entries :: [Entry.t()]
         ) :: State.t() do
    if is_nil(store0) or Enum.empty?(entries) do
      state
    else
      store1 = Store.write_log_entries(store0, entries)

      new_store =
        if Store.log_compaction_runnable?(store1) do
          index_next = logs.i_max + 1

          Store.switch_log_file_and_spawn_snapshot_writer(
            store1,
            make_snapshot(state),
            index_next
          )
        else
          store1
        end

      %State{state | store: new_store}
    end
  end

  defunp make_snapshot(%State{
           members: members,
           current_term: term,
           logs: logs,
           data: data,
           command_results: command_results,
           config: config
         }) :: Snapshot.t() do
    last_entry = Logs.last_committed_entry(logs)

    %Snapshot{
      members: members,
      term: term,
      last_committed_entry: last_entry,
      data: data,
      command_results: command_results,
      config: config
    }
  end

  defunp make_install_snapshot(%State{
           members: members,
           current_term: term,
           logs: logs,
           data: data,
           command_results: command_results,
           config: config
         }) :: InstallSnapshot.t() do
    last_entry = Logs.last_committed_entry(logs)

    %InstallSnapshot{
      members: members,
      term: term,
      last_committed_entry: last_entry,
      data: data,
      command_results: command_results,
      config: config
    }
  end

  defunp send_snapshot(
           %State{
             members: members,
             logs: logs,
             config: %Config{communication_module: mod},
             store: store
           } = state,
           follower :: pid,
           send_fun :: (module, InstallSnapshot.t() | InstallSnapshotCompressed.t() -> :ok)
         ) :: State.t() do
    # Assuming that `state` reflects all committed log entries.
    # Note that we avoid copying the entire `state` in order to minimize amount of data transfer to temporary process.
    case store do
      %Store{latest_snapshot_metadata: %SnapshotMetadata{path: path} = meta} ->
        spawn(fn ->
          send_fun.(mod, %InstallSnapshotCompressed{bin: File.read!(path)})
        end)

        %State{
          state
          | logs: Logs.set_follower_index_as_snapshot_last_index(logs, members, follower, meta)
        }

      _ ->
        send_fun.(mod, make_install_snapshot(state))
        state
    end
  end
end
