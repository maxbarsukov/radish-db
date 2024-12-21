defmodule RadishDB.Raft.Types.LogIndex do
  @moduledoc """
  Log index type.
  """

  use Croma.SubtypeOfInt, min: 0
end
