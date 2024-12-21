use Croma
alias Croma.TypeGen, as: TG

defmodule RadishDB.Raft.Types.RPC.AppendEntriesResponse do
  @moduledoc """
  Type for response on server's append entries
  """

  alias RadishDB.Raft.Types.{LogIndex, TermNumber}
  alias RadishDB.Raft.Utils.Monotonic

  use Croma.Struct,
    fields: [
      from: Croma.Pid,
      term: TermNumber,
      success: Croma.Boolean,
      i_replicated: TG.nilable(LogIndex),
      leader_timestamp: Monotonic
    ]
end
