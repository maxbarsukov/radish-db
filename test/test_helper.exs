use Croma

ExUnit.start()

defmodule MessageSendingHook do
  @moduledoc """
  Simple realization of `RadishDB.Raft.Communication.LeaderHook`
  """

  @behaviour RadishDB.Raft.Communication.LeaderHook
  def on_command_committed(_, _, _, _), do: nil
  def on_query_answered(_, _, _), do: nil
  def on_follower_added(_, pid), do: send(:test_runner, {:follower_added, pid})
  def on_follower_removed(_, pid), do: send(:test_runner, {:follower_removed, pid})
  def on_elected(_), do: send(:test_runner, {:elected, self()})
  def on_restored_from_files(_), do: send(:test_runner, {:restored_from_files, self()})
end

defmodule CommunicationWithNetsplit do
  @moduledoc """
  Simple realization of `RadishDB.Raft.Communication.Communicable`
  """

  @behaviour RadishDB.Raft.Communication.Communicable

  def start do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def set(pids) do
    Agent.update(__MODULE__, fn _ -> pids end)
  end

  defp reachable?(to) do
    isolated = Agent.get(__MODULE__, fn l -> l end)
    self() not in isolated and to not in isolated
  end

  def cast(server, event) do
    if reachable?(server) do
      :gen_statem.cast(server, event)
    else
      :ok
    end
  end

  def reply({to, _} = from, reply) do
    if reachable?(to) do
      :gen_statem.reply(from, reply)
    else
      :ok
    end
  end
end

defmodule RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker.Persist do
  @moduledoc """
  The `Persist` module implements the `PerMemberOptionsMaker` behavior
  to provide persistence options for Raft nodes in the consensus group.
  """

  alias RadishDB.ConsensusGroups.Cluster.Cluster
  alias RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker
  alias RadishDB.Raft.Node, as: RaftNode

  @behaviour PerMemberOptionsMaker
  defun make(name :: atom) :: [RaftNode.option()] do
    [persistence_dir: Path.join(["tmp", Atom.to_string(Node.self()), Atom.to_string(name)])]
  end
end

defmodule RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker.Raise do
  @moduledoc """
  The `Raise` module implements the `PerMemberOptionsMaker` behavior
  to provide fault injection options for Raft nodes in the consensus group.
  """

  alias RadishDB.ConsensusGroups.Cluster.Cluster
  alias RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker
  alias RadishDB.Raft.Node, as: RaftNode

  @behaviour PerMemberOptionsMaker
  defun make(name :: atom) :: [RaftNode.option()] do
    if name == Cluster do
      []
    else
      [test_inject_fault_after_add_follower: :raise]
    end
  end
end

defmodule RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker.Timeout do
  @moduledoc """
  The `Timeout` module implements the `PerMemberOptionsMaker` behavior
  to provide timeout options for Raft nodes in the consensus group.
  """

  alias RadishDB.ConsensusGroups.Cluster.Cluster
  alias RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker
  alias RadishDB.Raft.Node, as: RaftNode

  @behaviour PerMemberOptionsMaker
  defun make(name :: atom) :: [RaftNode.option()] do
    if name == Cluster do
      []
    else
      [test_inject_fault_after_add_follower: :timeout]
    end
  end
end

defmodule SlaveNode do
  @moduledoc """
  Slave node.
  """

  import ExUnit.Assertions

  alias RadishDB.ConsensusGroups.GroupApplication
  alias RadishDB.ConsensusGroups.Cluster.{Cluster, Manager}
  alias RadishDB.ConsensusGroups.OTP.ConsensusMemberSupervisor
  alias RadishDB.Raft.Node, as: RaftNode

  defmacro at(call, nodename) do
    {{:., _, [mod, fun]}, _, args} = call

    quote bind_quoted: [nodename: nodename, mod: mod, fun: fun, args: args] do
      if nodename == Node.self() do
        apply(mod, fun, args)
      else
        :rpc.call(nodename, mod, fun, args)
      end
    end
  end

  def with_slaves(shortnames, persist \\ :random, f) do
    Enum.each(shortnames, &start_slave(&1, persist))
    ret = f.()
    Enum.each(shortnames, &stop_slave/1)
    ret
  end

  def start_slave(shortname, persist \\ :random) do
    nodes_before = Node.list()
    {:ok, hostname} = :inet.gethostname()
    {:ok, longname} = :slave.start_link(hostname, shortname)
    true = :code.set_path(:code.get_path()) |> at(longname)

    case persist do
      :random -> PersistenceSetting.randomly_pick_whether_to_persist(longname)
      :yes -> PersistenceSetting.turn_on_persistence(longname)
      :no -> :ok
    end

    {:ok, _} = Application.ensure_all_started(:radish_db) |> at(longname)

    Enum.each(nodes_before, fn n ->
      Node.connect(n) |> at(longname)
    end)
  end

  def stop_slave(shortname) do
    :ok = :slave.stop(shortname_to_longname(shortname))
  end

  def shortname_to_longname(shortname) do
    {:ok, hostname} = :inet.gethostname()
    :"#{shortname}@#{hostname}"
  end

  def activate_node(node, zone_fun) do
    assert Supervisor.which_children(ConsensusMemberSupervisor) |> at(node) == []
    assert GroupApplication.deactivate() |> at(node) == {:error, :inactive}
    assert GroupApplication.activate(zone_fun.(node)) |> at(node) == :ok
    assert GroupApplication.activate(zone_fun.(node)) |> at(node) == {:error, :not_inactive}
    wait_for_activation(node, 10)
  end

  defp wait_for_activation(_, 0), do: raise("activation not completed!")

  defp wait_for_activation(node, tries_remaining) do
    state = :sys.get_state({Manager, node})

    if Manager.State.phase(state) == :active do
      :ok
    else
      :timer.sleep(1_000)
      wait_for_activation(node, tries_remaining - 1)
    end
  catch
    :exit, {:noproc, _} ->
      :timer.sleep(1_000)
      wait_for_activation(node, tries_remaining - 1)
  end

  def deactivate_node(node) do
    %{from: pid} =
      try do
        RaftNode.status({Cluster, node})
      catch
        :exit, _ ->
          # retry once again
          :timer.sleep(5_000)
          RaftNode.status({Cluster, node})
      end

    assert Process.alive?(pid) |> at(node)
    assert GroupApplication.deactivate() |> at(node) == :ok
    assert GroupApplication.deactivate() |> at(node) == {:error, :inactive}
    ref = Process.monitor(pid)
    assert_receive({:DOWN, ^ref, :process, ^pid, _reason}, 15_000)

    if node == Node.self() do
      # Wait for worker process to exit (if any)
      state = :sys.get_state(Manager)

      [state.adjust_worker, state.activate_worker, state.deactivate_worker]
      |> Enum.reject(&is_nil/1)
      |> Enum.each(fn p ->
        r = Process.monitor(p)
        assert_receive({:DOWN, ^r, :process, ^p, _reason}, 15_000)
      end)
    end
  end

  def with_active_nodes(nodes, zone_fun, f) do
    Enum.shuffle(nodes) |> Enum.each(&activate_node(&1, zone_fun))
    f.()
    Enum.shuffle(nodes) |> Enum.each(&deactivate_node/1)
  end

  def zone(node, n) do
    i = Atom.to_string(node) |> String.split("@") |> hd() |> String.to_integer() |> rem(n)
    "zone#{i}"
  end
end

defmodule PersistenceSetting do
  @moduledoc """
  Persistence settings
  """

  import SlaveNode, only: [at: 2]
  alias RadishDB.ConsensusGroups.Config.PerMemberOptionsMaker.Persist

  def randomly_pick_whether_to_persist(longname \\ Node.self()) do
    case :rand.uniform(2) do
      1 -> :ok
      2 -> turn_on_persistence(longname)
    end
  end

  def turn_on_persistence(longname) do
    Application.put_env(:radish_db, :per_member_options, Persist) |> at(longname)
  end
end

defmodule TestCaseTemplate do
  @moduledoc """
  Test case template
  """

  use ExUnit.CaseTemplate

  setup_all do
    Node.start(:"1", :shortnames)
    :ok
  end

  setup do
    # For clean testing we restart :radish_db
    case Application.stop(:radish_db) do
      :ok -> :ok
      {:error, {:not_started, :radish_db}} -> :ok
    end

    PersistenceSetting.randomly_pick_whether_to_persist()
    File.rm_rf!("tmp")

    :ok = Application.start(:radish_db)

    on_exit(fn ->
      Application.delete_env(:radish_db, :per_member_options)

      File.rm_rf!("tmp")
      # try to avoid slave start failures
      :timer.sleep(1000)
    end)
  end
end
