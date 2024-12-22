defmodule RadishDB.Raft.Persistence.PersistenceAndRecoveryTest do
  @moduledoc """
  Testing snapshots and log persistence
  """

  use ExUnit.Case, async: true
  alias RadishDB.Raft.Log.Entry
  alias RadishDB.Raft.Persistence.{Snapshot, Store}

  @config RadishDB.Raft.Node.make_config(JustAnInt, leader_hook_module: MessageSendingHook)
  @name :foo
  @tmp_dir "tmp"

  setup do
    File.rm_rf!(@tmp_dir)
    wait_until_other_test_runner_process_dead_and_register()

    on_exit(fn ->
      File.rm_rf!(@tmp_dir)
    end)
  end

  defp wait_until_other_test_runner_process_dead_and_register do
    ref = Process.monitor(:test_runner)

    receive do
      {:DOWN, ^ref, :process, _, _} -> :ok
    end

    Process.register(self(), :test_runner)
  end

  defp read_all_live_log_entries(i_committed) do
    log_paths = Store.find_log_files_containing_uncommitted_entries(@tmp_dir, i_committed)

    all_entries =
      Enum.flat_map(log_paths, fn path ->
        entries = Entry.read_as_stream(path) |> Enum.to_list()
        {_, index_first, _, _} = hd(entries)
        assert index_first == extract_first_index_from_log_path(path)
        entries
      end)

    Enum.map(all_entries, fn {_, i, _, _} -> i end)
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.each(fn [i1, i2] ->
      assert i2 - i1 == 1
    end)

    all_entries
  end

  defp extract_first_index_from_log_path(path) do
    ["log", index_first] = Path.basename(path) |> String.split("_")
    String.to_integer(index_first)
  end

  defp in_memory_and_disk_logs_same?(index_range) do
    logs = :sys.get_state(@name) |> elem(1) |> Map.fetch!(:logs)
    assert logs.i_min..logs.i_max == index_range
    assert Enum.sort(Map.keys(logs.map)) == Enum.to_list(index_range)
    i_committed = index_range.first

    m_disk =
      read_all_live_log_entries(i_committed)
      |> Enum.drop_while(fn {_, i, _, _} -> i < i_committed end)
      |> Map.new(fn {_, i, _, _} = e -> {i, e} end)

    # `logs.map` may contain extra entries which have been discarded in disk by log compaction
    assert m_disk == Map.take(logs.map, Map.keys(m_disk))
  end

  defp read_snapshot(path) do
    File.read!(path) |> Snapshot.decode()
  end

  defp snapshot_path_to_committed_index(path) do
    ["snapshot", _term, index] = Path.basename(path) |> String.split("_")
    String.to_integer(index)
  end

  test "persist logs and snapshots and recover by reading them" do
    {:ok, _} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        name: @name,
        persistence_dir: @tmp_dir
      )

    refute_received(_)
    assert in_memory_and_disk_logs_same?(1..1)
    assert :gen_statem.stop(@name) == :ok
    snapshot_path1 = Path.join(@tmp_dir, "snapshot_0_1")
    {_, snapshot_committed_index1, _, _} = read_snapshot(snapshot_path1).last_committed_entry
    assert snapshot_committed_index1 == 1

    {:ok, pid} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        name: @name,
        persistence_dir: @tmp_dir
      )

    assert_received({:restored_from_files, ^pid})

    assert in_memory_and_disk_logs_same?(
             snapshot_committed_index1..(snapshot_committed_index1 + 1)
           )

    assert RadishDB.Raft.Node.command(@name, :inc) == {:ok, 0}
    assert in_memory_and_disk_logs_same?(1..3)
    assert :gen_statem.stop(@name) == :ok

    {:ok, pid} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        name: @name,
        persistence_dir: @tmp_dir
      )

    assert_received({:restored_from_files, ^pid})

    assert in_memory_and_disk_logs_same?(
             snapshot_committed_index1..(snapshot_committed_index1 + 3)
           )

    Enum.each(1..40, fn i ->
      assert RadishDB.Raft.Node.command(@name, :inc) == {:ok, i}
    end)

    assert in_memory_and_disk_logs_same?(
             snapshot_committed_index1..(snapshot_committed_index1 + 43)
           )

    assert :gen_statem.stop(@name) == :ok

    [snapshot_path2] =
      Path.wildcard(Path.join(@tmp_dir, "snapshot_*")) |> List.delete(snapshot_path1)

    snapshot_committed_index2 = snapshot_path_to_committed_index(snapshot_path2)

    assert {_, ^snapshot_committed_index2, _, _} =
             read_snapshot(snapshot_path2).last_committed_entry

    {:ok, pid} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        name: @name,
        persistence_dir: @tmp_dir
      )

    assert_receive({:restored_from_files, ^pid})

    assert in_memory_and_disk_logs_same?(
             snapshot_committed_index2..(snapshot_committed_index1 + 44)
           )

    assert RadishDB.Raft.Node.query(@name, :get) == {:ok, 41}
    assert :gen_statem.stop(@name) == :ok
  end

  test "uncommitted logs should be committed by lonely leader immediately after recovery" do
    {:ok, l} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: @tmp_dir
      )

    {:ok, f} = RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [l]})
    assert MapSet.new(RadishDB.Raft.Node.status(l).members) == MapSet.new([l, f])
    assert :gen_statem.stop(f) == :ok

    # Now incoming commands won't be committed
    Enum.each(1..3, fn _ ->
      assert RadishDB.Raft.Node.command(l, :inc, 100) == {:error, :timeout}
    end)

    assert :gen_statem.stop(l) == :ok

    # Restore from snapshot, commands should be applied
    {:ok, _} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        name: @name,
        persistence_dir: @tmp_dir
      )

    assert RadishDB.Raft.Node.query(@name, :get) == {:ok, 3}
    assert :gen_statem.stop(@name) == :ok
  end

  test "follower should replicate log entries and store them in disk with de-duplication" do
    dir_l = Path.join(@tmp_dir, "l")
    dir_f = Path.join(@tmp_dir, "f")

    {:ok, l} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: dir_l
      )

    {:ok, f} =
      RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [l]}, persistence_dir: dir_f)

    # In case leader hasn't received AppendEntriesResponse from the follower, the leader re-sends part of log entries.
    # (This can happen also in non-persisting setup but is much more frequent in persisting setup
    # as followers must flush log entries before replying to its leader)
    assert RadishDB.Raft.Node.command(l, :inc) == {:ok, 0}

    # The follower should de-duplicate the received log entries before writing them to disk.
    [log_path] = Path.wildcard(Path.join(dir_f, "log_*"))
    entries = Entry.read_as_stream(log_path) |> Enum.to_list()
    assert entries == Enum.uniq(entries)

    assert :gen_statem.stop(f) == :ok
    assert :gen_statem.stop(l) == :ok
  end

  test "follower should reset its members field when recovery log entry is found" do
    dir_l = Path.join(@tmp_dir, "l")
    dir_f = Path.join(@tmp_dir, "f")

    {:ok, l1} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: dir_l
      )

    {:ok, f1} =
      RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [l1]},
        persistence_dir: dir_f
      )

    assert Enum.sort(RadishDB.Raft.Node.status(l1).members) == Enum.sort([l1, f1])
    assert Enum.sort(RadishDB.Raft.Node.status(f1).members) == Enum.sort([l1, f1])
    assert :gen_statem.stop(f1) == :ok
    assert :gen_statem.stop(l1) == :ok

    {:ok, l2} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: dir_l
      )

    {:ok, f2} =
      RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [l2]},
        persistence_dir: dir_f
      )

    assert Enum.sort(RadishDB.Raft.Node.status(l2).members) == Enum.sort([l2, f2])
    assert Enum.sort(RadishDB.Raft.Node.status(f2).members) == Enum.sort([l2, f2])
    assert :gen_statem.stop(f2) == :ok
    assert :gen_statem.stop(l2) == :ok
  end

  test "non-persisting and persisting members can interchange snapshots with each other" do
    {:ok, n1} = RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config})

    Enum.each(0..10, fn i ->
      assert RadishDB.Raft.Node.command(n1, :inc) == {:ok, i}
    end)

    # send snapshot: `n1` => `p1`
    {:ok, p1} =
      RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [n1]},
        persistence_dir: @tmp_dir
      )

    Enum.each(11..20, fn i ->
      assert RadishDB.Raft.Node.command(n1, :inc) == {:ok, i}
    end)

    assert :gen_statem.stop(p1) == :ok
    assert :gen_statem.stop(n1) == :ok

    # recover from disk snapshot
    {:ok, p2} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: @tmp_dir
      )

    Enum.each(21..30, fn i ->
      assert RadishDB.Raft.Node.command(p2, :inc) == {:ok, i}
    end)

    # send snapshot: `p2` => `n2`
    {:ok, n2} = RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [p2]})

    Enum.each(31..40, fn i ->
      assert RadishDB.Raft.Node.command(p2, :inc) == {:ok, i}
    end)

    assert :gen_statem.stop(n2) == :ok
    assert :gen_statem.stop(p2) == :ok
  end

  test "older snapshots should be discarded when making a new one" do
    {:ok, p1} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: @tmp_dir
      )

    Enum.each(0..100, fn i ->
      assert RadishDB.Raft.Node.command(p1, :inc) == {:ok, i}
    end)

    assert :gen_statem.stop(p1) == :ok
    snapshot_paths = Path.wildcard(Path.join(@tmp_dir, "snapshot_*"))
    assert length(snapshot_paths) == 2
    refute Path.join(@tmp_dir, "snapshot_0_1") in snapshot_paths
  end

  test "should recover from the latest snapshot and logs" do
    {:ok, p1} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: @tmp_dir
      )

    Enum.find(0..300, fn i ->
      assert RadishDB.Raft.Node.command(p1, :inc) == {:ok, i}

      snapshot_index =
        :sys.get_state(p1)
        |> elem(1)
        |> Map.fetch!(:store)
        |> Map.fetch!(:latest_snapshot_metadata)
        |> Map.fetch!(:last_committed_index)

      snapshot_index >= 100
    end)

    {:ok, n} = RadishDB.Raft.Node.query(p1, :get)
    assert :gen_statem.stop(p1) == :ok

    {:ok, p2} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: @tmp_dir
      )

    assert RadishDB.Raft.Node.query(p2, :get) == {:ok, n}
    assert :gen_statem.stop(p2) == :ok
  end

  test "should correctly reset current consensus group members on successful election" do
    dir_l = Path.join(@tmp_dir, "l")
    dir_f1 = Path.join(@tmp_dir, "f1")
    dir_f2 = Path.join(@tmp_dir, "f2")

    {:ok, l} =
      RadishDB.Raft.Node.start_link({:create_new_consensus_group, @config},
        persistence_dir: dir_l
      )

    {:ok, f1} =
      RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [l]},
        persistence_dir: dir_f1
      )

    assert :gen_statem.stop(f1) == :ok

    # Now 1/2 is running, leader has to step down.
    election_timeout_max = @config.election_timeout * 2
    :timer.sleep(election_timeout_max)
    assert RadishDB.Raft.Node.status(l).leader == nil

    # Forcibly remove the dead pid to restore leader.
    assert RadishDB.Raft.Node.force_remove_member(l, f1) == :ok
    :timer.sleep(election_timeout_max)
    assert RadishDB.Raft.Node.status(l).leader == l

    # Now add another member and it should understand the current members
    # (i.e. change due to force-remove should be recognized)
    {:ok, f2} =
      RadishDB.Raft.Node.start_link({:join_existing_consensus_group, [l]},
        persistence_dir: dir_f2
      )

    :timer.sleep(100)
    assert Enum.sort(RadishDB.Raft.Node.status(l).members) == Enum.sort([l, f2])
    assert Enum.sort(RadishDB.Raft.Node.status(f2).members) == Enum.sort([l, f2])

    assert :gen_statem.stop(l) == :ok
    assert :gen_statem.stop(f2) == :ok
  end
end
