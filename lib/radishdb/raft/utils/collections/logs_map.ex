use Croma

defmodule RadishDB.Raft.Utils.Collections.LogsMap do
  @moduledoc """
  A specialized map type used to store log entries in the Raft consensus algorithm.

  The `LogsMap` module is a subtype of a map that specifically uses `LogIndex`
  as keys and tuples as values, representing log entries.
  """

  alias RadishDB.Raft.Types.LogIndex

  use Croma.SubtypeOfMap, key_module: LogIndex, value_module: Croma.Tuple
end
