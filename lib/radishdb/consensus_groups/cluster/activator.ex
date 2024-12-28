use Croma

defmodule RadishDB.ConsensusGroups.Cluster.Activator do
  @moduledoc """
  The Activator module handles the activation process for nodes in the consensus group.
  """

  alias RadishDB.ConsensusGroups.Cluster.Cluster
  alias RadishDB.ConsensusGroups.GroupApplication
  alias RadishDB.ConsensusGroups.OTP.NodeReconnect

  @tries 5
  @sleep 1_000

  @doc """
  Activates a node in the specified zone by executing a series of steps.
  """
  def activate(zone) do
    steps = [
      :start_cluster_consensus_member,
      {:add_node, zone},
      :notify_node_reconnect_in_this_node,
      :notify_node_reconnects_in_other_nodes
    ]

    run_steps(steps, @tries)
  end

  defp run_steps(_, 0), do: raise("Failed to complete all steps of node activation!")
  defp run_steps([], _), do: :ok

  defp run_steps([s | ss], tries_remaining) do
    case step(s) do
      :ok ->
        run_steps(ss, tries_remaining)

      :error ->
        :timer.sleep(@sleep)
        run_steps([s | ss], tries_remaining - 1)
    end
  end

  defp step(:start_cluster_consensus_member) do
    case Supervisor.start_child(GroupApplication.Supervisor, Cluster.Server.child_spec()) do
      {:ok, _} -> :ok
      {:error, _} -> :error
    end
  end

  defp step({:add_node, zone}) do
    case GroupApplication.command(Cluster, {:add_node, Node.self(), zone}) do
      {:ok, _} -> :ok
      {:error, _} -> :error
    end
  end

  defp step(:notify_node_reconnect_in_this_node) do
    GenServer.cast(NodeReconnect, :this_node_activated)
  end

  defp step(:notify_node_reconnects_in_other_nodes) do
    GenServer.abcast(Node.list(), NodeReconnect, {:other_node_activated, Node.self()})
    :ok
  end
end
