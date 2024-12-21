defmodule RadishDB.Raft.Types.LogInfo do
  @moduledoc """
  Log info type.
  """

  alias RadishDB.Raft.Types.{LogIndex, TermNumber}

  use Croma.SubtypeOfTuple, elem_modules: [
    TermNumber,
    LogIndex
  ]
end
