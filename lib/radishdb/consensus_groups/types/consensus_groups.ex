use Croma

defmodule RadishDB.ConsensusGroups.Types.ConsensusGroups do
  @moduledoc """
  A module representing the ConsensusGroups type in the RadishDB consensus groups.
  """

  use Croma.SubtypeOfMap,
    key_module: Croma.Atom,
    value_module: Croma.PosInteger
end
