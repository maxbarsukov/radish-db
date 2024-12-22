use Croma

defmodule RadishDB.Raft.Types.RPC.RequestVoteResponse do
  @moduledoc """
  Type for response on server's request vote.
  """

  alias RadishDB.Raft.Types.TermNumber

  use Croma.Struct,
    fields: [
      from: Croma.Pid,
      term: TermNumber,
      vote_granted: Croma.Boolean
    ]
end
