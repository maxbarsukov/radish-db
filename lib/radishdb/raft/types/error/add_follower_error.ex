defmodule RadishDB.Raft.Types.Error.AddFollowerError do
  @moduledoc """
  Error on adding follower.
  """

  defexception [:message, :pid]
end
