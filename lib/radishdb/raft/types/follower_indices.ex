defmodule RadishDB.Raft.Types.FollowerIndices do
  @moduledoc """
  Follower indices type.
  """

  alias RadishDB.Raft.Types.LogIndex

  defmodule Pair do
    @moduledoc """
    Pair of log indies.
    """
    use Croma.SubtypeOfTuple, elem_modules: [LogIndex, LogIndex]
  end

  use Croma.SubtypeOfMap, key_module: Croma.Pid, value_module: Pair
end
