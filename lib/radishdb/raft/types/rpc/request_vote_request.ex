use Croma

defmodule RadishDB.Raft.Types.RPC.RequestVoteRequest do
  @moduledoc """
  Type for request on server's request vote.
  """

  alias RadishDB.Raft.Types.{LogInfo, TermNumber}

  use Croma.Struct,
    fields: [
      term: TermNumber,
      candidate_pid: Croma.Atom,
      last_log: LogInfo,
      replacing_leader: Croma.Boolean
    ]
end
