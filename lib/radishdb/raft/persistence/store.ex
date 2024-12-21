use Croma

defmodule RadishDB.Raft.Persistence.Store do
  @moduledoc """
  A module for managing the persistence of Raft log entries and snapshots.

  The `Store` module provides functionalities to handle the storage of Raft log entries and snapshots on disk.
  It allows for the creation of new stores with initial snapshots, writing log entries, and managing snapshots.
  This module is crucial for maintaining the durability and recoverability of the Raft consensus state.
  """

  alias RadishDB.Raft.Log.Entry
  alias RadishDB.Raft.Persistence.Snapshot
  alias RadishDB.Raft.Persistence.SnapshotMetadata
  alias RadishDB.Raft.Types.{LogIndex, TermNumber}

  use Croma.Struct, fields: [
    dir:                      Croma.String,
    # This field is `nil` only during initialization (within `new_with_initial_snapshotting/2`)
    log_fd:                   Croma.TypeGen.nilable(Croma.Tuple),
    log_size_written:         Croma.NonNegInteger,
    log_expansion_factor:     Croma.Number,
    # This field is `nil` only between startup and first snapshot
    latest_snapshot_metadata: Croma.TypeGen.nilable(SnapshotMetadata),
    snapshot_writer:          Croma.TypeGen.nilable(Croma.Pid),
  ]

  @doc """
  Creates a new persistence store with an initial snapshot.
  """
  defun new_with_initial_snapshotting(dir :: Path.t, factor :: number, snapshot :: Snapshot.t) :: t do
    File.mkdir_p!(dir)
    {_, index_first, _, _} = entry_elected = snapshot.last_committed_entry
    %__MODULE__{dir: dir, log_size_written: 0, log_expansion_factor: factor} # `log_fd` will be filled soon
    |> switch_log_file_and_spawn_snapshot_writer(snapshot, index_first)
    |> write_log_entries([entry_elected])
  end

  @doc """
  Initializes the store with a snapshot that has been loaded from disk.
  """
  defun new_with_disk_snapshot(
    dir :: Path.t,
    factor :: number,
    meta :: SnapshotMetadata.t,
    {_, index_first, _, _} = entry_restore :: LogEntry.t
  ) :: t do
    %__MODULE__{
      dir: dir,
      log_fd: open_log_file(dir, index_first),
      log_size_written: 0,
      log_expansion_factor: factor,
      latest_snapshot_metadata: meta
    } |> write_log_entries([entry_restore])
  end

  @doc """
  Initializes the store with a snapshot received from a leader.
  """
  defun new_with_snapshot_sent_from_leader(dir :: Path.t, factor :: number, snapshot :: Snapshot.t) :: t do
    File.mkdir_p!(dir)
    {_, index_snapshot, _, _} = snapshot.last_committed_entry
    %__MODULE__{dir: dir, log_size_written: 0, log_expansion_factor: factor} # `log_fd` will be filled soon
    |> switch_log_file_and_spawn_snapshot_writer(snapshot, index_snapshot + 1)
  end

  @doc """
  Removes the latest snapshot metadata from the store.
  """
  defun unset_snapshot_metadata(p :: t) :: t do
    %__MODULE__{p | latest_snapshot_metadata: nil}
  end

  @doc """
  Writes log entries to the log file.
  """
  defun write_log_entries(%__MODULE__{log_fd: fd, log_size_written: size} = p, entries :: [LogEntry.t]) :: t do
    bin = Enum.map(entries, &LogEntry.to_binary/1) |> :erlang.iolist_to_binary()
    :ok = :file.write(fd, bin)
    %__MODULE__{p | log_size_written: size + byte_size(bin)}
  end

  @doc """
  Determines if log compaction can be performed based on the current state.
  """
  defun log_compaction_runnable?(
    %__MODULE__{
      latest_snapshot_metadata: meta,
      log_size_written: size_l,
      log_expansion_factor: factor,
      snapshot_writer: writer}
  ) :: boolean do
    if is_pid(writer) do
      false
    else
      case meta do
        nil                             -> true
        %SnapshotMetadata{size: size_s} -> size_s * factor < size_l
      end
    end
  end

  @doc """
  Switches to a new log file and spawns a process to write the snapshot.
  """
  defun switch_log_file_and_spawn_snapshot_writer(
    %__MODULE__{dir: dir, log_fd: log_fd1} = persistence,
    snapshot   :: Snapshot.t,
    index_next :: LogIndex.t
  ) :: t do
    if log_fd1 do
      :ok = :file.close(log_fd1)
    end
    server_pid  = self()
    log_fd2     = open_log_file(dir, index_next)
    {pid, _ref} = spawn_monitor(fn ->
      write_snapshot(snapshot, dir, server_pid)
    end)
    %__MODULE__{persistence | log_fd: log_fd2, log_size_written: 0, snapshot_writer: pid}
  end

    @doc """
  Reads the last log index from the log files in the specified directory.
  """
  defun read_last_log_index(dir :: Path.t) :: nil | LogIndex.t do
    case Path.wildcard(Path.join(dir, "log_*")) do
      []    -> nil
      paths ->
        latest_log_path = Enum.max_by(paths, &extract_first_log_index_from_path/1)
        LogEntry.read_last_entry_index(latest_log_path)
    end
  end

  defp open_log_file(dir, index_next) do
    log_path = Path.join(dir, "log_#{index_next}")
    File.open!(log_path, [:write, :sync, :raw])
  end

  defp write_snapshot(
    %Snapshot{term: term, last_committed_entry: {_, last_committed_index, _, _}} = snapshot,
    dir,
    server_pid
  ) do
    snapshot_basename = "snapshot_#{term}_#{last_committed_index}"
    snapshot_path     = Path.join(dir, snapshot_basename)
    compressed        = Snapshot.encode(snapshot)
    File.write!(snapshot_path, compressed)

    # notify the gen_statem process
    # (we have to wait for reply in order to ensure that older snapshots won't be used anymore)
    message = {:snapshot_created, snapshot_path, term, last_committed_index, byte_size(compressed)}
    :ok = :gen_statem.call(server_pid, message, :infinity)

    # cleanup obsolete snapshots and logs
    find_obsolete_snapshots(dir, snapshot_basename)
    |> Enum.each(&File.rm!/1)
    find_log_files_with_committed_entries_only(dir, last_committed_index)
    |> Enum.each(&File.rm!/1)
  end

  defunp find_obsolete_snapshots(dir :: Path.t, snapshot_basename :: String.t) :: [Path.t] do
    list_snapshots_in(dir)
    |> Enum.filter(fn path -> Path.basename(path) != snapshot_basename end)
    |> Enum.drop(1) # Keep one more latest snapshot
  end

  defun list_snapshots_in(dir :: Path.t) :: [Path.t] do
    Path.wildcard(Path.join(dir, "snapshot_*"))
    |> Enum.sort_by(&snapshot_path_to_commit_index/1, &>=/2) # newest first
  end

  defunp snapshot_path_to_commit_index(p :: Path.t) :: LogIndex.t do
    ["snapshot", _term, index] = Path.basename(p) |> String.split("_")
    String.to_integer(index)
  end

  defunp find_log_files_with_committed_entries_only(dir :: Path.t, i_committed :: LogIndex.t) :: [Path.t] do
    partition_obsolete_and_live_log_files(dir, i_committed)
    |> elem(0)
    |> Enum.map(&elem(&1, 0))
  end

  defun find_log_files_containing_uncommitted_entries(dir :: Path.t, i_committed :: LogIndex.t) :: [Path.t] do
    partition_obsolete_and_live_log_files(dir, i_committed)
    |> elem(1)
    |> Enum.map(&elem(&1, 0))
  end

  defunp partition_obsolete_and_live_log_files(
    dir :: Path.t,
    i_committed :: LogIndex.t
  ) :: {[{Path.t, LogIndex.t}], [{Path.t, LogIndex.t}]} do
    Path.wildcard(Path.join(dir, "log_*"))
    |> Enum.map(fn path -> {extract_first_log_index_from_path(path), path} end)
    |> Enum.sort()
    |> Enum.chunk_every(2, 1, [nil])
    |> Enum.map(fn
      [{_, path}, {i_next, _}] -> {path, i_next - 1}
      [{_, path}, nil        ] -> {path, :infinity } # atom is larger than any integers
    end)
    |> Enum.split_with(fn {_, i} -> i < i_committed end)
  end

  defunp extract_first_log_index_from_path(path :: Path.t) :: LogIndex.t do
    ["log", index_first] = Path.basename(path) |> String.split("_")
    String.to_integer(index_first)
  end
end
