use Croma

defmodule RadishDB.Raft.Utils.Monotonic do
  @moduledoc """
  Representation of monotonic time via `System.monotonic_time/0` which never decreases and does not leap.
  """

  @type t :: integer
  defun(valid?(v :: term) :: boolean, do: is_integer(v))

  defun milliseconds() :: t do
    System.monotonic_time(:millisecond)
  end
end
