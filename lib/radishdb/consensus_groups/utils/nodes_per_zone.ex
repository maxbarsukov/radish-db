use Croma
alias Croma.TypeGen, as: TG

defmodule RadishDB.ConsensusGroups.Utils.NodesPerZone do
  @moduledoc """
  Provides utility functions for managing and processing nodes organized by zones
  within a consensus group in the RadishDB system.

  This module defines a subtype of map where keys are zone identifiers and values
  are lists of nodes associated with those zones. It includes functions for selecting
  members based on specific criteria.

  ## Functions

    * `lrw_members/3` - Retrieves a specified number of nodes from various zones,
      sorted and indexed by a hash calculated from the node and a task identifier.
  """

  alias RadishDB.ConsensusGroups.Types.ZoneId
  alias RadishDB.Utils.Hash

  use Croma.SubtypeOfMap, key_module: ZoneId, value_module: TG.list_of(Croma.Atom)

  @doc """
  Retrieves a list of nodes from the given `nodes_per_zone` based on the specified
  `task_id`. The nodes are sorted by a hash calculated from each node and the task ID,
  and only the first `n_to_take` nodes are returned.

  ## Parameters

    - `nodes_per_zone`: A map where keys are zone IDs and values are lists of nodes.
    - `task_id`: An identifier for the task, used for hash calculations
        - a group name (atom) but any ID that can be assigned to node(s) is acceptable.
    - `n_to_take`: The number of nodes to retrieve, must be a positive integer.

  ## Returns

  A list of nodes sorted by their calculated hash values, limited to `n_to_take` nodes.
  """
  defun lrw_members(
          nodes_per_zone :: t,
          task_id :: atom | any,
          n_to_take :: pos_integer
        ) :: [node] do
    Enum.flat_map(nodes_per_zone, fn {_z, ns} ->
      Enum.map(ns, fn n -> {Hash.calc({n, task_id}), n} end)
      |> Enum.sort()
      |> Enum.map_reduce(0, fn {hash, node}, index -> {{index, hash, node}, index + 1} end)
      |> elem(0)
    end)
    |> Enum.sort()
    |> Enum.take(n_to_take)
    |> Enum.map(fn {_i, _h, n} -> n end)
  end
end
