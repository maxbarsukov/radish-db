defmodule RadishDB.Raft.Persistence.SnapshotMetadata do
  @moduledoc """
  Snapshot metadata type.
  """

  alias RadishDB.Raft.Types.{LogIndex, TermNumber}

  use Croma.Struct, fields: [
    path: Croma.String,
    term: TermNumber,
    size: Croma.PosInteger,
    last_committed_index: LogIndex,
  ]
end
