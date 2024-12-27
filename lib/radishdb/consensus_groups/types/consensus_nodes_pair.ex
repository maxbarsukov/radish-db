use Croma
alias Croma.TypeGen, as: TG

defmodule RadishDB.ConsensusGroups.Types.ConsensusNodesPair do
  @moduledoc """
  A module representing the ConsensusNodesPair type in the RadishDB consensus groups.
  """

  use Croma.SubtypeOfTuple, elem_modules: [Croma.Atom, TG.list_of(Croma.Atom)]
end
