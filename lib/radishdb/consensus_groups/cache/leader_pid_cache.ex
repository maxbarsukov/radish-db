use Croma

defmodule RadishDB.ConsensusGroups.Cache.LeaderPidCache do
  @moduledoc """
  A module for caching leader PIDs using ETS.

  This module provides functions to initialize the cache, set and get leader PIDs,
  unset them, and retrieve all keys in the cache.
  """

  alias RadishDB.ConsensusGroups.Cache.Ets

  @table_name :leader_pid_cache

  defun(init() :: :ok, do: Ets.init(@table_name))

  defun(get(name :: atom) :: nil | pid, do: Ets.get(@table_name, name))

  defun(set(name :: atom, leader :: nil | pid) :: :ok, do: Ets.set(@table_name, name, leader))

  defun(unset(name :: atom) :: :ok, do: Ets.unset(@table_name, name))

  defun(keys() :: [atom], do: Ets.keys(@table_name))

  defun find_leader_and_cache(name :: atom) :: nil | pid do
    case find_leader(name) do
      nil ->
        nil

      pid ->
        set(name, pid)
        pid
    end
  end

  defunp find_leader(name :: atom) :: nil | pid do
    [Node.self() | Node.list()]
    |> Enum.map(fn node -> try_status({name, node}) end)
    |> Enum.filter(&match?(%{leader: p} when is_pid(p), &1))
    |> case do
      [] -> nil
      ss -> Enum.max_by(ss, & &1.current_term).leader
    end
  end

  def try_status(server) do
    RadishDB.Raft.Node.status(server) |> Map.delete(:config)
  catch
    :exit, _ -> nil
  end

  def retrieve_member_statuses(group) do
    [Node.self() | Node.list()]
    |> Enum.map(fn n -> {n, try_status({group, n})} end)
    |> Enum.reject(&match?({_, nil}, &1))
  end
end
