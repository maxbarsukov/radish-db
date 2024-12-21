defmodule RadishDB.Raft.Types.Error.RedundantSizeInformationError do
  @moduledoc """
  Error for redundant size information in entry.
  """

  defexception [:message, :pid]
end
