use Croma

defmodule RadishDB.ConsensusGroups.Cache.Ets do
  @moduledoc """
  A module that serves as a wrapper for managing data in an Erlang Term Storage (ETS) table.
  """

  defun init(table_name :: atom) :: :ok do
    :ets.new(table_name, [:public, :named_table, {:read_concurrency, true}])
    :ok
  end

  defun get(table_name :: atom, key :: atom) :: nil | pid do
    case :ets.lookup(table_name, key) do
      [] -> nil
      [{_, value}] -> value
    end
  end

  defun set(table_name :: atom, key :: atom, value :: nil | pid) :: :ok do
    :ets.insert(table_name, {key, value})
    :ok
  end

  defun unset(table_name :: atom, key :: atom) :: :ok do
    :ets.delete(table_name, key)
    :ok
  end

  defun keys(table_name :: atom) :: [atom] do
    case :ets.first(table_name) do
      :"$end_of_table" -> []
      k -> collect_keys(table_name, k, [])
    end
  end

  defp collect_keys(table_name, k, acc) do
    case :ets.next(table_name, k) do
      :"$end_of_table" -> Enum.reverse(acc)
      next_key -> collect_keys(table_name, next_key, [next_key | acc])
    end
  end
end
