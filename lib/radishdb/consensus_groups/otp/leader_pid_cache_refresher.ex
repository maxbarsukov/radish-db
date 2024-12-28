use Croma

defmodule RadishDB.ConsensusGroups.OTP.LeaderPidCacheRefresher do
  @moduledoc """
  A GenServer responsible for periodically refreshing
  the leader PID caches stored locally within the system.
  """

  use GenServer

  alias RadishDB.ConsensusGroups.Cache.LeaderPidCache
  alias RadishDB.ConsensusGroups.Cluster
  alias RadishDB.ConsensusGroups.Config.Config
  alias RadishDB.ConsensusGroups.GroupApplication

  defun start_link([]) :: {:ok, pid} do
    GenServer.start_link(__MODULE__, :ok, [])
  end

  def init(:ok) do
    {:ok, nil, Config.leader_pid_cache_refresh_interval()}
  end

  def handle_info(:timeout, nil) do
    refresh_all()
    {:noreply, nil, Config.leader_pid_cache_refresh_interval()}
  end

  def handle_info(_delayed_reply, nil) do
    {:noreply, nil, Config.leader_pid_cache_refresh_interval()}
  end

  defp refresh_all do
    LeaderPidCache.keys() |> MapSet.new() |> refresh_all_by_keys
  end

  defp refresh_all_by_keys(keys) when map_size(keys) == 0 do
    :ok
  end

  defp refresh_all_by_keys(keys) do
    nodes = MapSet.new(Node.list())
    cache_keys = MapSet.delete(keys, Cluster)
    group_names = GroupApplication.consensus_groups() |> MapSet.new(fn {name, _} -> name end)

    MapSet.intersection(cache_keys, group_names) |> Enum.each(&confirm_leader_or_find(&1, nodes))
    MapSet.difference(cache_keys, group_names) |> Enum.each(&LeaderPidCache.unset/1)

    MapSet.difference(group_names, cache_keys)
    |> Enum.each(&LeaderPidCache.find_leader_and_cache/1)
  end

  defp confirm_leader_or_find(name, nodes) do
    with pid when is_pid(pid) <- LeaderPidCache.get(name),
         true <- MapSet.member?(nodes, node(pid)),
         %{state_name: :leader} <- LeaderPidCache.try_status(pid) do
      :ok
    else
      _ -> LeaderPidCache.find_leader_and_cache(name)
    end
  end
end
