use Croma

defmodule RadishDB.ConsensusGroups.Config.RaftConfigConstructor do
  @moduledoc """
  A behaviour module for constructing Raft configuration objects.
  """

  alias RadishDB.Raft.Types.Config

  @callback make(consensus_group_name :: atom) :: Config.t()
end
