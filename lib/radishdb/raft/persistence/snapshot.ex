use Croma

defmodule RadishDB.Raft.Persistence.Snapshot do
  @moduledoc """
  Represents a snapshot of the Raft consensus state.

  The `Snapshot` module handles the state of the Raft consensus algorithm at a specific point in time,
  including the current term, the last committed log entry, and the configuration of the members.
  It provides functionality to encode and decode snapshots, as well as to read the latest snapshot
  and associated log entries from disk.
  """

  alias RadishDB.Raft.Communication.Members
  alias RadishDB.Raft.Log.Entry
  alias RadishDB.Raft.Persistence.SnapshotMetadata
  alias RadishDB.Raft.Persistence.Store
  alias RadishDB.Raft.Types.RPC.InstallSnapshot
  alias RadishDB.Raft.Types.{CommandResults, Config, TermNumber}

  use Croma.Struct,
    fields: [
      members: Members,
      term: TermNumber,
      last_committed_entry: Entry,
      config: Config,
      data: Croma.Any,
      command_results: CommandResults
    ]

  @doc """
  Converts an `InstallSnapshot` message into a `Snapshot`.
  """
  defun from_install_snapshot(is :: InstallSnapshot.t()) :: t do
    Map.put(is, :__struct__, __MODULE__)
  end

  @doc """
  Reads the latest snapshot and log files from the specified directory,
  returning the snapshot, metadata, and log entries if available.
  """
  defun read_latest_snapshot_and_logs_if_available(dir :: Path.t()) ::
          nil | {t, SnapshotMetadata.t(), Enumerable.t(Entry.t())} do
    case find_snapshot_and_log_files(dir) do
      nil ->
        nil

      {snapshot_path, meta, log_paths} ->
        snapshot = File.read!(snapshot_path) |> decode()
        {_, last_committed_index, _, _} = snapshot.last_committed_entry

        log_stream =
          Stream.flat_map(log_paths, &Entry.read_as_stream/1)
          |> Stream.drop_while(fn {_, i, _, _} -> i < last_committed_index end)

        {snapshot, meta, log_stream}
    end
  end

  defunp find_snapshot_and_log_files(dir :: Path.t()) ::
           nil | {Path.t(), SnapshotMetadata.t(), [Path.t()]} do
    case Store.list_snapshots_in(dir) |> List.first() do
      nil ->
        nil

      snapshot_path ->
        ["snapshot", term_str, last_index_str] = Path.basename(snapshot_path) |> String.split("_")
        last_committed_index = String.to_integer(last_index_str)
        %File.Stat{size: size} = File.stat!(snapshot_path)

        meta = %SnapshotMetadata{
          path: snapshot_path,
          term: String.to_integer(term_str),
          last_committed_index: last_committed_index,
          size: size
        }

        log_paths = Store.find_log_files_containing_uncommitted_entries(dir, last_committed_index)
        {snapshot_path, meta, log_paths}
    end
  end

  @doc """
  Encodes a snapshot into a binary format for storage.
  """
  defun encode(snapshot :: t) :: binary do
    :erlang.term_to_binary(snapshot) |> :zlib.gzip()
  end

  @doc """
  Decodes a binary representation back into a snapshot.
  """
  defun decode(bin :: binary) :: t do
    :zlib.gunzip(bin) |> :erlang.binary_to_term()
  end
end
