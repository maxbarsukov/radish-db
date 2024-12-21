use Croma
alias Croma.TypeGen, as: TG
alias Croma.Result, as: R

defmodule RadishDB.Raft.Communication.Members do
  @moduledoc """
  The `RadishDB.Raft.Communication.Members` module manages the membership of nodes in a Raft consensus algorithm implementation.

  It keeps track of the current leader, all members in the cluster, and any pending or uncommitted membership changes.

  ## Structure

  The module defines a struct with the following fields:

  - `leader`: The PID of the current leader node. This can be `nil` if there is no leader.
  - `all`: A set of PIDs representing all members in the cluster, which is replicated using Raft logs.
  - `uncommitted_membership_change`: An optional entry representing a membership change that has not yet been committed.
  - `pending_leader_change`: An optional PID representing a leader change that is pending.
  """

  alias RadishDB.Raft.Log.Entry
  alias RadishDB.Raft.Types.LogIndex
  alias RadishDB.Raft.Utils.Collections.PidSet

  use Croma.Struct, fields: [
    leader:                        TG.nilable(Croma.Pid),
    all:                           PidSet, # replicated using raft logs (i.e. reproducible from logs)
    uncommitted_membership_change: TG.nilable(Entry),  # replicated using raft logs (i.e. reproducible from logs)
    pending_leader_change:         TG.nilable(Croma.Pid),
  ]

  @doc """
  Creates a new instance of the members struct for a lonely leader (the only member in the cluster).

  ## Examples

      iex> RadishDB.Raft.Communication.Members.new_for_lonely_leader()
      %RadishDB.Raft.Communication.Members{leader: #PID<0.123.0>, all: #PidSet<...>, uncommitted_membership_change: nil, pending_leader_change: nil}
  """
  defun new_for_lonely_leader() :: t do
    %__MODULE__{leader: self(), all: PidSet.put(PidSet.new(), self()), uncommitted_membership_change: nil}
  end

  @doc """
  Returns a list of PIDs of members other than the current node.

  ## Parameters

    - members: The members struct.

  ## Examples

      iex> members = %RadishDB.Raft.Communication.Members{all: PidSet.put(PidSet.new(), self())}
      iex> RadishDB.Raft.Communication.Members.other_members_list(members)
      []
  """
  defun other_members_list(%__MODULE__{all: all}) :: [pid] do
    PidSet.delete(all, self()) |> PidSet.to_list()
  end

  @doc """
  Updates the leader field, discarding any pending leader change.

  ## Parameters

    - m: The current members struct.
    - leader_or_nil: The new leader PID or `nil`.

  ## Examples

      iex> members = %RadishDB.Raft.Communication.Members{leader: nil}
      iex> RadishDB.Raft.Communication.Members.put_leader(members, self())
      %RadishDB.Raft.Communication.Members{leader: #PID<0.123.0>, ...}
  """
  defun put_leader(m :: t, leader_or_nil :: nil | pid) :: t do
    # when resetting `leader`, `pending_leader_change` field should be discarded (if any)
    %__MODULE__{m | leader: leader_or_nil, pending_leader_change: nil}
  end

  @doc """
  Initiates adding a new follower to the cluster, ensuring no ongoing membership or leader changes.

  ## Parameters

    - m: The current members struct.
    - entry: The log entry containing the command to add a follower.

  ## Returns

    - `{:error, reason}` if the addition cannot be performed.
    - The updated members struct if the addition is successful.

  ## Examples

      iex> members = %RadishDB.Raft.Communication.Members{all: PidSet.new()}
      iex> RadishDB.Raft.Communication.Members.start_adding_follower(members, {1, 1, :add_follower, new_follower_pid})
      %RadishDB.Raft.Communication.Members{...}
  """
  defun start_adding_follower(%__MODULE__{all: all} = m,
                              {_term, _index, :add_follower, new_follower} = entry) :: R.t(t) do
    reject_if_membership_changing(m, fn ->
      reject_if_leader_changing(m, fn ->
        if PidSet.member?(all, new_follower) do
          {:error, :already_joined}
        else
          %__MODULE__{m | all: PidSet.put(all, new_follower), uncommitted_membership_change: entry} |> R.pure()
        end
      end)
    end)
  end

  @doc """
  Initiates removing a follower from the cluster, preventing the removal of the leader or a non-member.

  ## Parameters

    - m: The current members struct.
    - entry: The log entry containing the command to remove a follower.

  ## Returns

    - `{:error, reason}` if the removal cannot be performed.
    - The updated members struct if the removal is successful.

  ## Examples

      iex> members = %RadishDB.Raft.Communication.Members{all: PidSet.put(PidSet.new(), old_follower_pid)}
      iex> RadishDB.Raft.Communication.Members.start_removing_follower(members, {1, 1, :remove_follower, old_follower_pid})
      %RadishDB.Raft.Communication.Members{...}
  """
  defun start_removing_follower(%__MODULE__{all: all} = m,
                                {_term, _index, :remove_follower, old_follower} = entry) :: R.t(t) do
    reject_if_membership_changing(m, fn ->
      reject_if_leader_changing(m, fn ->
        cond do
          old_follower == self()            -> {:error, :cannot_remove_leader}
          PidSet.member?(all, old_follower) -> %__MODULE__{m | all: PidSet.delete(all, old_follower), uncommitted_membership_change: entry} |> R.pure()
          true                              -> {:error, :not_member}
        end
      end)
    end)
  end

  @doc """
  Initiates replacing the current leader with a new leader, ensuring the new leader is a member.

  ## Parameters

    - m: The current members struct.
    - new_leader: The PID of the new leader or `nil`.

  ## Returns

    - `{:error, reason}` if the replacement cannot be performed.
    - The updated members struct if the replacement is successful.

  ## Examples

      iex> members = %RadishDB.Raft.Communication.Members{all: PidSet.new()}
      iex> RadishDB.Raft.Communication.Members.start_replacing_leader(members, new_leader_pid)
      %RadishDB.Raft.Communication.Members{...}
  """
  defun start_replacing_leader(%__MODULE__{all: all} = m,
                               new_leader :: nil | pid) :: R.t(t) do
    reject_if_membership_changing(m, fn ->
      cond do
        is_nil(new_leader)              -> %__MODULE__{m | pending_leader_change: nil} |> R.pure()
        new_leader == self()            -> {:error, :already_leader}
        PidSet.member?(all, new_leader) -> %__MODULE__{m | pending_leader_change: new_leader} |> R.pure()
        true                            -> {:error, :not_member}
      end
    end)
  end

  @doc """
  Marks a membership change as committed based on the log index.

  ## Parameters

    - m: The current members struct.
    - index: The log index indicating the commitment.

  ## Returns

    - The updated members struct.

  ## Examples

      iex> members = %RadishDB.Raft.Communication.Members{uncommitted_membership_change: {1, 1, :add_follower, follower_pid}}
      iex> RadishDB.Raft.Communication.Members.membership_change_committed(members, 1)
      %RadishDB.Raft.Communication.Members{...}
  """
  defun membership_change_committed(%__MODULE__{uncommitted_membership_change: change} = m, index :: LogIndex.t) :: t do
    case change do
      {_, i, _, _} when i <= index -> %__MODULE__{m | uncommitted_membership_change: nil}
      _                            -> m
    end
  end

  @doc """
  Forces the removal of a member from the cluster, cleaning up any associated uncommitted changes.

  ## Parameters

    - m: The current members struct.
    - pid: The PID of the member to be removed.

  ## Returns

    - The updated members struct after forcing the removal.

  ## Examples

      iex> members = %RadishDB.Raft.Communication.Members{all: PidSet.put(PidSet.new(), pid_to_remove)}
      iex> RadishDB.Raft.Communication.Members.force_remove_member(members, pid_to_remove)
      %RadishDB.Raft.Communication.Members{...}
  """
  defun force_remove_member(
    %__MODULE__{
      all: all,
      uncommitted_membership_change: mchange,
      pending_leader_change: lchange
    } = m, pid :: pid
  ) :: t do
    mchange2 =
      case mchange do
        {_term, _index, :add_follower   , ^pid} -> nil
        {_term, _index, :remove_follower, ^pid} -> nil
        _                                       -> mchange
      end
    %__MODULE__{m |
      all:                           PidSet.delete(all, pid),
      uncommitted_membership_change: mchange2,
      pending_leader_change:         (if lchange == pid, do: nil, else: lchange),
    }
  end

  defp reject_if_membership_changing(%__MODULE__{uncommitted_membership_change: change}, f) do
    if change do
      {:error, :uncommitted_membership_change}
    else
      f.()
    end
  end

  defp reject_if_leader_changing(%__MODULE__{pending_leader_change: change}, f) do
    if change do
      {:error, :pending_leader_change}
    else
      f.()
    end
  end
end
