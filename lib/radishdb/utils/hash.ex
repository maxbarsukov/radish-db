use Croma

defmodule RadishDB.Utils.Hash do
  @moduledoc """
  A module for hashing values using the `:erlang.phash2/2` function.

  This module defines a type that represents an integer in the range from 0 to 2^32 - 1,
  and provides the `calc/1` function, which computes the hash for a given value.
  """

  @pow_2_32 4_294_967_296
  use Croma.SubtypeOfInt, min: 0, max: @pow_2_32 - 1

  @doc """
  Computes the hash for a given value.

  ## Parameters

    - `value`: Any value for which the hash needs to be computed.

  ## Returns

  Returns an integer representing the hash of the value in the range from 0 to 2^32 - 1.
  """
  defun calc(value :: term) :: t do
    :erlang.phash2(value, @pow_2_32)
  end
end
