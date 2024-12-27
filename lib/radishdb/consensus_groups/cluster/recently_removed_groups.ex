use Croma

defmodule RadishDB.ConsensusGroups.Cluster.RecentlyRemovedGroups do
  @moduledoc """
  A module for managing recently removed consensus groups in RadishDB.

  This module keeps track of active nodes and their corresponding groups,
  allowing for efficient management of recently removed groups and their
  indices.

  ## Structure

  The main structure includes:
    - `active_nodes`: A map of currently active nodes.
    - `min_index`: The minimum index of the groups.
    - `max_index`: The maximum index of the groups.
    - `index_to_group`: A mapping from indices to group names.
    - `group_to_indices`: A mapping from group names to their indices.
  """

  alias RadishDB.ConsensusGroups.Utils.NodesPerZone

  defmodule NodesMap do
    @moduledoc """
    A module representing a map of nodes with their corresponding indices.

    Each entry consists of a node and a tuple containing a timestamp and
    an optional index.
    """

    defmodule Pair do
      @moduledoc """
      A tuple representing a pair of a positive integer and an optional
      positive integer.

      This tuple is used to track the state of nodes, where the first
      element is a timestamp and the second element is an optional index.
      """
      use Croma.SubtypeOfTuple,
        elem_modules: [Croma.PosInteger, Croma.TypeGen.nilable(Croma.PosInteger)]
    end

    use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: Pair
  end

  defmodule IndexToGroupName do
    @moduledoc """
    A module representing a mapping from indices to group names.

    Each entry consists of a positive integer as the key and an atom
    representing the group name as the value.
    """
    use Croma.SubtypeOfMap, key_module: Croma.PosInteger, value_module: Croma.Atom
  end

  defmodule GroupNameToIndices do
    @moduledoc """
    A module representing a mapping from group names to their indices.

    Each entry consists of an atom as the key (group name) and a list of
    positive integers as the value (indices).
    """
    use Croma.SubtypeOfMap,
      key_module: Croma.Atom,
      value_module: Croma.TypeGen.list_of(Croma.PosInteger)
  end

  use Croma.Struct,
    fields: [
      # This field contains both currently active nodes and nodes that were recently active
      active_nodes: NodesMap,
      min_index: Croma.TypeGen.nilable(Croma.PosInteger),
      max_index: Croma.TypeGen.nilable(Croma.PosInteger),
      index_to_group: IndexToGroupName,
      group_to_indices: GroupNameToIndices
    ]

  @doc """
  Creates an empty `RecentlyRemovedGroups` struct.

  ## Returns

  A new instance of `RecentlyRemovedGroups` with all fields initialized.
  """
  defun empty() :: t do
    %__MODULE__{
      active_nodes: %{},
      min_index: nil,
      max_index: nil,
      index_to_group: %{},
      group_to_indices: %{}
    }
  end

  @doc """
  Adds a new group to the structure.

  ## Parameters

    - t: The current state of the `RecentlyRemovedGroups`.
    - group: The name of the group to add.

  ## Returns

  The updated `RecentlyRemovedGroups` struct with the new group added.
  """
  defun add(
          %__MODULE__{min_index: min, max_index: max, index_to_group: i2g, group_to_indices: g2is} =
            t,
          group :: atom
        ) :: t do
    case {min, max} do
      {nil, nil} ->
        %__MODULE__{
          t
          | min_index: 1,
            max_index: 1,
            index_to_group: %{1 => group},
            group_to_indices: %{group => [1]}
        }

      _ ->
        i = max + 1
        new_i2g = Map.put(i2g, i, group)
        new_g2is = Map.update(g2is, group, [i], &[i | &1])
        %__MODULE__{t | max_index: i, index_to_group: new_i2g, group_to_indices: new_g2is}
    end
  end

  @doc """
  Checks if there are ongoing cleanup operations for a given group.

  ## Parameters

    - t: The current state of the `RecentlyRemovedGroups`.
    - group: The name of the group to check.

  ## Returns

  A boolean indicating whether there are ongoing cleanup operations.
  """
  defun cleanup_ongoing?(%__MODULE__{min_index: min, group_to_indices: g2is}, group :: atom) ::
          boolean do
    Map.get(g2is, group, [])
    |> Enum.any?(fn i -> min < i end)
  end

  @doc """
  Cancels the specified group from the structure.

  This function is retained for backward compatibility and should be
  removed in future releases.

  ## Parameters

    - t: The current state of the `RecentlyRemovedGroups`.
    - group: The name of the group to cancel.

  ## Returns

  The updated `RecentlyRemovedGroups` struct with the group cancelled.
  """
  defun cancel(%__MODULE__{index_to_group: i2g, group_to_indices: g2is} = t, group :: atom) :: t do
    {is, new_g2is} = Map.pop(g2is, group, [])
    new_i2g = Enum.reduce(is, i2g, fn i, m -> Map.delete(m, i) end)
    %__MODULE__{t | index_to_group: new_i2g, group_to_indices: new_g2is}
  end

  @doc """
  Retrieves the names of groups associated with a given node.

  ## Parameters

    - t: The current state of the `RecentlyRemovedGroups`.
    - node_from: The node for which to retrieve group names.

  ## Returns

  A tuple containing a list of group names and the maximum index.
  """
  defun names_for_node(
          %__MODULE__{active_nodes: nodes, min_index: min, max_index: max, index_to_group: i2g},
          node_from :: node
        ) :: {[atom], nil | pos_integer} do
    case min do
      nil ->
        {[], nil}

      _ ->
        index_from =
          case Map.get(nodes, node_from) do
            nil -> min
            {_t, nil} -> min
            {_t, index} -> index + 1
          end

        if index_from <= max do
          names = Enum.map(index_from..max, fn i -> Map.get(i2g, i) end) |> Enum.reject(&is_nil/1)
          {names, max}
        else
          {[], nil}
        end
    end
  end

  @doc """
  Updates the state of `RecentlyRemovedGroups` based on the current nodes and their indices.

  ## Parameters

    - t: The current state of the `RecentlyRemovedGroups`.
    - npz: The nodes per zone structure.
    - node_from: The node being updated.
    - index_or_nil: The index of the node or nil.
    - now: The current time in milliseconds.
    - wait_time: The time to wait before considering a node inactive.

  ## Returns

  The updated `RecentlyRemovedGroups` struct after the update.
  """
  defun update(
          t :: t,
          npz :: NodesPerZone.t(),
          node_from :: node,
          index_or_nil :: nil | pos_integer,
          now :: pos_integer,
          wait_time :: pos_integer
        ) :: t do
    t
    |> touch_currently_active_nodes(npz, now)
    |> forget_about_nodes_that_had_been_deactivated(now - wait_time)
    |> set_node_index(node_from, index_or_nil)
    |> proceed_min_index()
  end

  defp touch_currently_active_nodes(%__MODULE__{active_nodes: nodes} = t, npz, now) do
    currently_active_nodes = Enum.flat_map(npz, fn {_z, ns} -> ns end)

    new_nodes =
      Enum.reduce(currently_active_nodes, nodes, fn n, ns ->
        index_or_nil =
          case Map.get(ns, n) do
            nil -> nil
            {_time, index} -> index
          end

        Map.put(ns, n, {now, index_or_nil})
      end)

    %__MODULE__{t | active_nodes: new_nodes}
  end

  defp forget_about_nodes_that_had_been_deactivated(
         %__MODULE__{active_nodes: nodes} = t,
         threshold
       ) do
    new_nodes = Enum.reject(nodes, fn {_n, {t, _i}} -> t < threshold end) |> Map.new()
    %__MODULE__{t | active_nodes: new_nodes}
  end

  defp set_node_index(%__MODULE__{active_nodes: nodes} = t, node_from, index_or_nil) do
    new_nodes =
      case index_or_nil do
        nil -> nodes
        index when is_integer(index) -> update_node_index(nodes, node_from, index)
      end

    %__MODULE__{t | active_nodes: new_nodes}
  end

  defp update_node_index(nodes, node_from, index) do
    case Map.get(nodes, node_from) do
      nil -> nodes
      {time, _index} -> Map.put(nodes, node_from, {time, index})
    end
  end

  defp proceed_min_index(
         %__MODULE__{
           active_nodes: nodes,
           min_index: min,
           index_to_group: i2g0,
           group_to_indices: g2is0
         } = t
       ) do
    smallest_node_index = calculate_smallest_node_index(nodes, min)

    if min < smallest_node_index do
      {new_index_to_group, new_group_to_indices} =
        update_indices_and_groups(min, smallest_node_index, i2g0, g2is0)

      %__MODULE__{
        t
        | min_index: smallest_node_index,
          index_to_group: new_index_to_group,
          group_to_indices: new_group_to_indices
      }
    else
      t
    end
  end

  defp calculate_smallest_node_index(nodes, min) do
    nodes
    |> Enum.map(fn {_node, {_timestamp, index}} -> index || min end)
    |> Enum.min(fn -> min end)
  end

  defp update_indices_and_groups(min, smallest_node_index, i2g0, g2is0) do
    Enum.reduce(min..(smallest_node_index - 1), {i2g0, g2is0}, fn index,
                                                                  {current_i2g, current_g2is} ->
      case Map.pop(current_i2g, index) do
        # No group found for this index, leave unchanged
        {nil, _} ->
          {current_i2g, current_g2is}

        {group, updated_i2g} ->
          updated_g2is = update_group_indices(group, index, current_g2is)
          # Return updated maps
          {updated_i2g, updated_g2is}
      end
    end)
  end

  defp update_group_indices(group, index, current_g2is) do
    case Map.fetch!(current_g2is, group) |> List.delete(index) do
      # Remove group if no indices left
      [] -> Map.delete(current_g2is, group)
      # Update indices for the group
      indices -> Map.put(current_g2is, group, indices)
    end
  end
end
