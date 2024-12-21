defmodule RadishDB.Raft.Types.TermNumber do
  @moduledoc """
  Term index type.
  """

  use Croma.SubtypeOfInt, min: 0
end
