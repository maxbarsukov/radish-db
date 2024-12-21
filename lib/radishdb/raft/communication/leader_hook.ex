defmodule RadishDB.Raft.Communication.LeaderHook do
  @moduledoc """
  A behaviour module for hooks that are invoked in leader.

  Note that there are cases where hooks are invoked multiple times for a single event due to leader change.
  """

  alias RadishDB.Raft.StateMachine.Statable

  @type neglected :: any

  @doc """
  Hook to be called when a command submitted by `RadishDB.Raft.Node.command/4` is committed.
  """
  @callback on_command_committed(
              data_before_command :: Statable.data(),
              command_arg :: Statable.command_arg(),
              command_ret :: Statable.command_ret(),
              data_after_command :: Statable.data()
            ) :: neglected

  @doc """
  Hook to be called when a query given by `RadishDB.Raft.Node.query/3` is executed.
  """
  @callback on_query_answered(
              data :: Statable.data(),
              query_arg :: Statable.query_arg(),
              query_ret :: Statable.query_ret()
            ) :: neglected

  @doc """
  Hook to be called when a new follower is added to a consensus group
  by `RadishDB.Raft.Node.start_link/2` with `:join_existing_consensus_group` specified.
  """
  @callback on_follower_added(Statable.data(), pid) :: neglected

  @doc """
  Hook to be called when a follower is removed from a consensus group by `RadishDB.Raft.Node.remove_follower/2`.
  """
  @callback on_follower_removed(Statable.data(), pid) :: neglected

  @doc """
  Hook to be called when a new leader is elected in a consensus group.
  """
  @callback on_elected(Statable.data()) :: neglected

  @doc """
  Hook to be called when a new leader restores its state from snapshot & log files.
  """
  @callback on_restored_from_files(Statable.data()) :: neglected
end

defmodule RadishDB.Raft.Communication.LeaderHook.NoOp do
  @moduledoc """
  Leader hook implementation with no operations.
  """

  @behaviour RadishDB.Raft.Communication.LeaderHook
  def on_command_committed(_, _, _, _), do: nil
  def on_query_answered(_, _, _), do: nil
  def on_follower_added(_, _), do: nil
  def on_follower_removed(_, _), do: nil
  def on_elected(_), do: nil
  def on_restored_from_files(_), do: nil
end
