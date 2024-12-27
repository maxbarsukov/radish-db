use Croma
alias Croma.TypeGen, as: TG

defmodule RadishDB.ConsensusGroups.Types.MembersPerLeaderNode do
  @moduledoc """
  A module representing the MembersPerLeaderNode type in the RadishDB consensus groups.
  """

  alias RadishDB.ConsensusGroups.Types.ConsensusNodesPair

  use Croma.SubtypeOfMap,
    key_module: Croma.Atom,
    value_module: TG.list_of(ConsensusNodesPair)
end
