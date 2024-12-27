defmodule RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker do
  @moduledoc """
  Defines a behaviour for modules that provide the creation of per-member options
  for consensus groups in a Raft-based system.
  """

  alias RadishDB.Raft.Node

  @callback make(consensus_group_name :: atom) :: [Node.option()]
end
