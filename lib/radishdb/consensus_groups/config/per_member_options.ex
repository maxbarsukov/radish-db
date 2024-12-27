use Croma

defmodule RadishDB.ConsensusGroups.Config.PerMemberOptions do
  @moduledoc """
  Provides functionality to build per-member options for consensus groups in a Raft-based system.
  """

  alias RadishDB.ConsensusGroups.Config.Config
  alias RadishDB.Raft.Node

  defun build(name :: atom) :: [Node.option()] do
    case Config.per_member_options() do
      nil -> []
      mod -> mod.make(name)
    end
    |> Keyword.put(:name, name)
  end
end
