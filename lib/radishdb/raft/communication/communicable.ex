use Croma

defmodule RadishDB.Raft.Communication.Communicable do
  @moduledoc """
  Defines the communication interface for RadishDB.Raft.

  This module provides a behavior for communication between nodes in a Raft cluster. It specifies the callbacks that must be implemented for sending messages and replies.

  ## Callbacks

  - `cast/2`: Sends a message to a server asynchronously.
  - `reply/2`: Sends a reply to a specific process.

  Implementing this behavior allows for flexibility in how messages are sent and received, enabling different communication strategies if needed.
  """
  @callback cast(server :: GenServer.server(), msg :: any) :: :ok

  @callback reply(from :: GenServer.from(), reply :: any) :: :ok
end

defmodule RadishDB.Raft.Communication.RemoteMessageGateway do
  @moduledoc """
  Default implementation of `:communication_module` for RadishDB.Raft.

  This module provides a way to send messages to other nodes in a Raft cluster, handling the potential issues that arise from slow message passing to unreachable nodes.

  ## Functionality

  - `cast/2` behaves like `:gen_statem.cast/2`, allowing asynchronous message passing.
  - `reply/2` behaves like `:gen_statem.reply/2`, enabling replies to messages sent to processes.

  This is introduced in order to work-around slow message passing to unreachable nodes;
  if the target node is not connected to `Node.self()`, functions in this node give up delivering message.
  In order to recover from temporary network issues, reconnecting to disconnected nodes should be tried elsewhere.
  """

  @behaviour RadishDB.Raft.Communication.Communicable

  def cast(fsm_ref, event) do
    do_send(fsm_ref, {:"$gen_cast", event})
  end

  def reply({pid, tag}, reply) do
    do_send(pid, {tag, reply})
  end

  defp do_send(dest, msg) do
    :erlang.send(dest, msg, [:noconnect])
    :ok
  end
end
