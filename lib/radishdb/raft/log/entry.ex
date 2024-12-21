use Croma

defmodule RadishDB.Raft.Log.Entry do
  @moduledoc """
  The `RadishDB.Raft.Log.Entry` module defines the structure and functionality for log entries in the Raft consensus algorithm.

  Log entries represent commands, queries, configuration changes, leader elections, and membership changes.
  Each entry is associated with a term number and a log index.

  ## Types

  The log entry type `t` is defined as one of the follows:

  - `{TermNumber.t, LogIndex.t, :command, {GenServer.from, Statable.command_arg, reference}}`
  - `{TermNumber.t, LogIndex.t, :query, {GenServer.from, Statable.query_arg}}`
  - `{TermNumber.t, LogIndex.t, :change_config, Config.t}`
  - `{TermNumber.t, LogIndex.t, :leader_elected, [pid]}`
  - `{TermNumber.t, LogIndex.t, :add_follower, pid}`
  - `{TermNumber.t, LogIndex.t, :remove_follower, pid}`
  - `{TermNumber.t, LogIndex.t, :restore_from_files, pid}`
  """

  alias RadishDB.Raft.StateMachine.Statable
  alias RadishDB.Raft.Types.{Config, LogIndex, TermNumber}
  alias RadishDB.Raft.Types.Error.RedundantSizeInformationError

  @type t ::
          {TermNumber.t(), LogIndex.t(), :command,
           {GenServer.from(), Statable.command_arg(), reference}}
          | {TermNumber.t(), LogIndex.t(), :query, {GenServer.from(), Statable.query_arg()}}
          | {TermNumber.t(), LogIndex.t(), :change_config, Config.t()}
          | {TermNumber.t(), LogIndex.t(), :leader_elected, [pid]}
          | {TermNumber.t(), LogIndex.t(), :add_follower, pid}
          | {TermNumber.t(), LogIndex.t(), :remove_follower, pid}
          | {TermNumber.t(), LogIndex.t(), :restore_from_files, pid}

  defun valid?(v :: any) :: boolean do
    {_, _, _, _} -> true
    _ -> false
  end

  defp entry_type_to_tag(:command), do: 0
  defp entry_type_to_tag(:query), do: 1
  defp entry_type_to_tag(:change_config), do: 2
  defp entry_type_to_tag(:leader_elected), do: 3
  defp entry_type_to_tag(:add_follower), do: 4
  defp entry_type_to_tag(:remove_follower), do: 5
  defp entry_type_to_tag(:restore_from_files), do: 6

  defp tag_to_entry_type(0), do: {:ok, :command}
  defp tag_to_entry_type(1), do: {:ok, :query}
  defp tag_to_entry_type(2), do: {:ok, :change_config}
  defp tag_to_entry_type(3), do: {:ok, :leader_elected}
  defp tag_to_entry_type(4), do: {:ok, :add_follower}
  defp tag_to_entry_type(5), do: {:ok, :remove_follower}
  defp tag_to_entry_type(6), do: {:ok, :restore_from_files}
  defp tag_to_entry_type(_), do: :error

  @doc """
  Serializes a log entry into a binary format.

  ## Parameters

    - entry: The log entry to serialize.

  ## Returns

    - The serialized binary representation of the log entry.

  ## Examples

      iex> entry = {1, 1, :command, {self(), :some_command, make_ref()}}
      iex> RadishDB.Raft.Log.Entry.to_binary(entry)
      <<...>>  # binary representation
  """
  defun to_binary({term, index, entry_type, others} :: t) :: binary do
    bin = :erlang.term_to_binary(others)
    size = byte_size(bin)

    <<
      term::size(64),
      index::size(64),
      entry_type_to_tag(entry_type)::size(8),
      size::size(64),
      bin::binary,
      size::size(64)
    >>
  end

  defunpt extract_from_binary(bin :: binary) :: nil | {t, rest :: binary} do
    with <<term::size(64), index::size(64), type_tag::size(8), size1::size(64)>> <> rest1 <- bin,
         {:ok, entry_type} = tag_to_entry_type(type_tag),
         <<others_bin::binary-size(size1), size2::size(64)>> <> rest2 <- rest1 do
      if size1 == size2 do
        {{term, index, entry_type, :erlang.binary_to_term(others_bin)}, rest2}
      else
        raise RedundantSizeInformationError,
          message: "redundant size information in log entry not matched"
      end
    else
      # insufficient input, can be retried with subsequent binary data
      _ -> nil
    end
  end

  @doc """
  Reads log entries as a stream from a specified log file.

  ## Parameters

    - log_path: The path to the log file.

  ## Returns

    - A stream of log entries.

  ## Examples

      iex> entries = RadishDB.Raft.Log.Entry.read_as_stream("log_file_path")
      # Stream of log entries
  """
  def read_as_stream(log_path) do
    File.stream!(log_path, [], 4096)
    |> Stream.transform(<<>>, fn bin, carryover ->
      extract_multiple_from_binary(carryover <> bin)
    end)
  end

  defunp extract_multiple_from_binary(bin :: binary) :: {[t], rest :: binary} do
    extract_multiple_from_binary_impl(bin, [])
  end

  defp extract_multiple_from_binary_impl(bin, acc) do
    case extract_from_binary(bin) do
      nil -> {Enum.reverse(acc), bin}
      {entry, rest} -> extract_multiple_from_binary_impl(rest, [entry | acc])
    end
  end

  @doc """
  Reads the index of the last log entry from a specified log file.

  ## Parameters

    - log_path: The path to the log file.

  ## Returns

    - `nil` if the log is empty or an error occurs, otherwise returns the index of the last log entry.

  ## Examples

      iex> last_index = RadishDB.Raft.Log.Entry.read_last_entry_index("log_file_path")
      5
  """
  defun read_last_entry_index(log_path :: Path.t()) :: nil | LogIndex.t() do
    case :file.open(log_path, [:raw, :binary]) do
      {:ok, f} -> read_last_entry_index_impl(f, File.stat!(log_path).size)
      {:error, _} -> nil
    end
  end

  defp read_last_entry_index_impl(_f, size) when size < 8, do: nil

  defp read_last_entry_index_impl(f, size) do
    {:ok, <<binsize1::size(64)>>} = :file.pread(f, size - 8, 8)

    # (term: 8, index: 8, tag: 1, size1: 8, size2: 8) -> 33
    last_entry_start_offset = size - binsize1 - 33
    read_last_entry_index_by_start_offset(f, binsize1, last_entry_start_offset)
  end

  defp read_last_entry_index_by_start_offset(_f, _binsize1, last_entry_start_offset)
       when last_entry_start_offset < 0,
       do: nil

  defp read_last_entry_index_by_start_offset(f, binsize1, last_entry_start_offset) do
    {:ok,
     <<
       _term::size(64),
       index::size(64),
       type_tag::size(8),
       binsize2::size(64)
     >>} = :file.pread(f, last_entry_start_offset, 25)

    # minimal sanity checking
    case tag_to_entry_type(type_tag) do
      {:ok, _} when binsize2 == binsize1 -> index
      _ -> nil
    end
  end
end
