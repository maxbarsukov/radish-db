defmodule RadishDB.ConsensusGroups.Cluster.RecoveryTest do
  @moduledoc """
  Testing cluster recovery
  """

  use ExUnit.Case

  alias RadishDB.ConsensusGroups.GroupApplication
  alias RadishDB.ConsensusGroups.Cluster.Cluster
  alias RadishDB.ConsensusGroups.OTP.ConsensusMemberSupervisor

  defmodule ConfigMaker do
    @moduledoc """
    Example of Raft config maker
    """

    alias RadishDB.ConsensusGroups.Config.RaftConfigConstructor
    alias RadishDB.Raft.Node, as: RaftNode

    @behaviour RaftConfigConstructor

    @impl true
    def make(name) do
      case name do
        Cluster -> Cluster.make_raft_config()
        _                 ->
          RaftNode.make_config(JustAnInt, [
            heartbeat_timeout: 500,
            # Disk I/O is sometimes rather slow, resulting in more frequent leader elections
            election_timeout: 2500,
          ])
      end
    end
  end

  setup do
    # For clean testing we restart :radish_db
    case Application.stop(:radish_db) do
      :ok                                   -> :ok
      {:error, {:not_started, :radish_db}} -> :ok
    end
    PersistenceSetting.turn_on_persistence(Node.self())
    Application.put_env(:radish_db, :raft_config, ConfigMaker)
    File.rm_rf!("tmp")
    :ok = Application.start(:radish_db)
    on_exit(fn ->
      Application.delete_env(:radish_db, :per_member_options)
      File.rm_rf!("tmp")
      # try to avoid slave start failures in travis
      :timer.sleep(1000)
    end)
  end

  defp start_activate_stop(f) do
    :ok = Application.ensure_started(:radish_db)
    :ok = GroupApplication.activate("zone")
    f.()
    :ok = Application.stop(:radish_db)
  end

  test "recovery from files should restore all previously existed consensus groups" do
    start_activate_stop(fn ->
      :ok
    end)

    start_activate_stop(fn ->
      assert GroupApplication.consensus_groups == %{}
      assert Supervisor.which_children(ConsensusMemberSupervisor) == []
      assert GroupApplication.add_consensus_group(:c1) == :ok
      [{_, pid, _, _}] = Supervisor.which_children(ConsensusMemberSupervisor)
      assert Process.info(pid)[:registered_name] == :c1
    end)
    refute Process.whereis(:c1)

    start_activate_stop(fn ->
      assert GroupApplication.consensus_groups() == %{c1: 3}
      [{_, pid, _, _}] = Supervisor.which_children(ConsensusMemberSupervisor)
      assert Process.info(pid)[:registered_name] == :c1
      :ok = GroupApplication.remove_consensus_group(:c1)
    end)
    refute Process.whereis(:c1)

    start_activate_stop(fn ->
      assert GroupApplication.consensus_groups() == %{}
      assert Supervisor.which_children(ConsensusMemberSupervisor) == []
    end)
  end
end
