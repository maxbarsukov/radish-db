defmodule RadishDB.Raft.Types.CommandId do
  @moduledoc """
  Represents the identifier for commands in the Raft consensus algorithm.

  This module defines the type for command identifiers, which can be either a reference or any other type.
  This flexibility allows for various implementations of command identifiers depending on the specific needs of the Raft protocol.
  """

  @type t :: reference | any
end
