use Croma

defmodule RadishDB.Raft.Types.CommandResults do
  @moduledoc """
  Represents the results of commands processed in the Raft consensus algorithm.

  This module provides a structure for storing command results along with their identifiers in a queue.
  It ensures that results can be efficiently managed and retrieved while maintaining a maximum size limit for the stored results.
  """

  alias RadishDB.Raft.Types.CommandId

  @type cmd_id :: CommandId.t
  @type t :: {:queue.queue(cmd_id), %{cmd_id => any}}

  defun valid?(v :: any) :: boolean do
    {{l1, l2}, m} when is_list(l1) and is_list(l2) and is_map(m) -> true
    _ -> false
  end

  @doc """
  Creates a new empty command results structure.
  """
  defun new() :: t do
    {:queue.new(), %{}}
  end

  @doc """
  Retrieves the result associated with a given command identifier, returning `{:ok, result}` or `:error` if not found.
  """
  defun fetch({_q, m} :: t, cmd_id :: cmd_id) :: {:ok, any} | :error do
    Map.fetch(m, cmd_id)
  end

  @doc """
  Adds a new command result to the structure, managing the queue and map size according to the specified maximum size.
  """
  defun put({q1, m1} :: t,
            cmd_id   :: cmd_id,
            result   :: any,
            max_size :: pos_integer) :: t do
    q2 = :queue.in(cmd_id, q1)
    m2 = Map.put(m1, cmd_id, result)
    if map_size(m2) <= max_size do
      {q2, m2}
    else
      {{:value, cmd_id_removed}, q3} = :queue.out(q2)
      m3 = Map.delete(m2, cmd_id_removed)
      {q3, m3}
    end
  end
end
