use Croma

defmodule RadishDB.ConsensusGroups.OTP.ConsensusMemberSupervisor do
  @moduledoc """
  A supervisor for managing consensus members in the RadishDB system.

  This module uses a simple one-for-one strategy to supervise the
  individual consensus members, allowing for dynamic start and stop
  of members as needed.

  ## Supervision Strategy

  - **Strategy**: `:simple_one_for_one` - Each child process can be
    started and restarted independently.
  - **Intensity**: 3 - The maximum number of restarts allowed within
    the specified period.
  - **Period**: 5 seconds - The time window for the restart intensity.

  The child processes are represented by the `Wrapper` module, which
  encapsulates the logic for starting a consensus member.
  """

  alias RadishDB.ConsensusGroups.Config.PerMemberOptions
  alias RadishDB.Raft.Node

  @behaviour :supervisor

  defmodule Wrapper do
    @moduledoc """
    Wrapper for Raft node.
    """

    defun start_link(group_info :: Node.consensus_group_info(), name :: atom) ::
            GenServer.on_start() do
      Node.start_link(group_info, PerMemberOptions.build(name))
    end
  end

  defun start_link() :: {:ok, pid} do
    :supervisor.start_link({:local, __MODULE__}, __MODULE__, :ok)
  end

  @impl true
  def init(:ok) do
    sup_flags = %{
      strategy: :simple_one_for_one,
      intensity: 3,
      period: 5
    }

    worker_spec = %{
      id: Wrapper,
      start: {Wrapper, :start_link, []},
      type: :worker,
      restart: :temporary,
      shutdown: 5000
    }

    {:ok, {sup_flags, [worker_spec]}}
  end

  defun child_spec([]) :: Supervisor.child_spec() do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end
end
