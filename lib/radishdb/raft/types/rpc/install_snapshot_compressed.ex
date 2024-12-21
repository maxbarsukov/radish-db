use Croma

defmodule RadishDB.Raft.Types.RPC.InstallSnapshotCompressed do
  @moduledoc """
  Represents a compressed snapshot installation request in the Raft consensus algorithm.

  This struct is used to hold a binary representation of a snapshot that has been compressed for
  efficient transmission over the network. The compressed format reduces the amount of data that needs to be sent,
  which is particularly useful for large snapshots.

  ## Fields

  - `bin`: The binary data representing the compressed snapshot.
  """

  use Croma.Struct,
    fields: [
      bin: Croma.Binary
    ]
end
