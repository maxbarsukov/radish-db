use Croma

defmodule RadishDB.Raft.Types.RPC.TimeoutNow do
  @moduledoc """
  Represents a timeout request for immediate processing in the Raft consensus algorithm.

  This struct is used to signal that the current leader should immediately process an `AppendEntriesRequest`.
  This can be useful in scenarios where the leader needs to expedite the handling of log entries or heartbeat messages.

  ## Fields

  - `append_entries_req`: The `AppendEntriesRequest` that needs to be processed immediately.
  """

  alias RadishDB.Raft.Types.RPC.AppendEntriesRequest

  use Croma.Struct,
    fields: [
      append_entries_req: AppendEntriesRequest
    ]
end
