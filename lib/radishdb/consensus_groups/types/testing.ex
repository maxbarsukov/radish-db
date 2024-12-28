if Mix.env() == :test do
  # To run code within slave nodes during tests,
  # all modules must be compiled into beam files (i.e. they can't load .exs files)

  defmodule JustAnInt do
    @moduledoc """
    Simple realization of `RadishDB.Raft.StateMachine.Statable`
    """

    @behaviour RadishDB.Raft.StateMachine.Statable
    def new, do: 0
    def command(i, {:set, j}), do: {i, j}
    def command(i, :inc), do: {i, i + 1}
    def query(i, :get), do: i
  end
end
