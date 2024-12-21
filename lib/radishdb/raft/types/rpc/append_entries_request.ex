use Croma
alias Croma.TypeGen, as: TG

defmodule RadishDB.Raft.Types.RPC.AppendEntriesRequest do
  @moduledoc """
  Type for request on server's append entries
  """

  alias RadishDB.Raft.Log.Entry
  alias RadishDB.Raft.Types.{LogIndex, LogInfo, TermNumber}
  alias RadishDB.Raft.Utils.Monotonic

  use Croma.Struct,
    fields: [
      leader_pid: Croma.Pid,
      term: TermNumber,
      prev_log: LogInfo,
      entries: TG.list_of(Entry),
      i_leader_commit: LogIndex,
      leader_timestamp: Monotonic
    ]
end
