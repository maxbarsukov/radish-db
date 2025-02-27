defmodule RadishDB.Raft.Log.EntryTest do
  @moduledoc """
  Test for RadishDB.Raft.Log.Entry
  """

  use Croma.TestCase
  alias RadishDB.Raft.Persistence.{SnapshotMetadata, Store}

  @dir Path.join("tmp", "log_entry_test")

  setup do
    File.rm_rf!(@dir)
    File.mkdir_p!(@dir)

    on_exit(fn ->
      File.rm_rf!(@dir)
    end)
  end

  defp make_gen_server_from do
    {self(), make_ref()}
  end

  defp make_entries_list do
    [
      {1, 2, :command, {make_gen_server_from(), :some_command_arg, make_ref()}},
      {1, 2, :query, {make_gen_server_from(), :some_query_arg}},
      {1, 2, :change_config, RadishDB.Raft.Node.make_config(__MODULE__)},
      {1, 2, :leader_elected, self()},
      {1, 2, :add_follower, self()},
      {1, 2, :remove_follower, self()},
      {1, 2, :restore_from_files, self()}
    ]
  end

  test "Entry.t <=> binary representation" do
    make_entries_list()
    |> Enum.each(fn entry ->
      binary = Entry.to_binary(entry)

      [
        <<>>,
        :crypto.strong_rand_bytes(16)
      ]
      |> Enum.each(fn rest ->
        assert Entry.extract_from_binary(binary <> rest) == {entry, rest}
      end)
    end)
  end

  test "extract_from_binary/1 should report error on failure" do
    bin = :erlang.term_to_binary(self())
    size = byte_size(bin)

    [
      <<>>,
      # insufficient data
      <<1::size(64), 2::size(64), 3::size(8), size + 10::size(64), bin::binary,
        size + 10::size(64)>>
    ]
    |> Enum.each(fn b ->
      assert Entry.extract_from_binary(b) == nil
    end)

    [
      # invalid tag
      <<1::size(64), 2::size(64), 8::size(8), size::size(64), bin::binary, size::size(64)>>,
      # :erlang.binary_to_term/1 fails
      <<1::size(64), 2::size(64), 3::size(8), 7::size(64), "invalid", 7::size(64)>>,
      # sizes not match
      <<1::size(64), 2::size(64), 3::size(8), size::size(64), bin::binary, size + 1::size(64)>>
    ]
    |> Enum.each(fn b ->
      catch_error(Entry.extract_from_binary(b))
    end)
  end

  test "read_as_stream/1" do
    File.mkdir_p!(@dir)
    initial_entry = {0, 1, :leader_elected, [self()]}

    meta = %SnapshotMetadata{
      path: Path.join(@dir, "snapshot_0_1"),
      term: 0,
      last_committed_index: 1,
      size: 100
    }

    store1 = Store.new_with_disk_snapshot(@dir, 10, meta, initial_entry)

    l = make_entries_list()
    entries = Enum.map(1..100, fn _ -> Enum.random(l) end)
    store2 = Store.write_log_entries(store1, entries)
    assert store2.log_size_written >= 4096

    [log_path] = Path.wildcard(Path.join(@dir, "log_*"))
    stream = Entry.read_as_stream(log_path)
    assert Enum.to_list(stream) == [initial_entry | entries]
  end

  test "read_last_entry_index/1" do
    index_first = :rand.uniform(1000)
    initial_entry = {0, index_first, :leader_elected, [self()]}

    meta = %SnapshotMetadata{
      path: Path.join(@dir, "snapshot_0_#{index_first}"),
      term: 0,
      last_committed_index: 1,
      size: 100
    }

    store1 = Store.new_with_disk_snapshot(@dir, 10, meta, initial_entry)

    n_entries = :rand.uniform(10)
    index_last = index_first + n_entries
    l = make_entries_list()
    entries = Enum.map(1..n_entries, fn i -> put_elem(Enum.random(l), 1, index_first + i) end)
    _store2 = Store.write_log_entries(store1, entries)

    [log_path] = Path.wildcard(Path.join(@dir, "log_*"))
    assert Entry.read_last_entry_index(log_path) == index_last
    assert Store.read_last_log_index(@dir) == index_last
  end

  test "read_last_entry_index/1 should return nil on failure" do
    assert Entry.read_last_entry_index(Path.join(@dir, "nonexisting")) == nil
    assert RadishDB.Raft.Node.read_last_log_index(@dir) == nil

    bin = :erlang.term_to_binary(self())
    size = byte_size(bin)

    [
      "",
      <<"abcdef", 1000::size(64)>>,
      # invalid tag
      <<1::size(64), 2::size(64), 8::size(8), size::size(64), bin::binary, size::size(64)>>,
      # sizes not match
      <<1::size(64), 2::size(64), 3::size(8), size + 1::size(64), bin::binary, size::size(64)>>
    ]
    |> Enum.each(fn content ->
      path = Path.join(@dir, "log")
      File.write!(path, content)
      assert Entry.read_last_entry_index(path) == nil
      assert RadishDB.Raft.Node.read_last_log_index(@dir) == nil
    end)
  end
end
