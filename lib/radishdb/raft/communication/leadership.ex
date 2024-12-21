use Croma

defmodule RadishDB.Raft.Communication.Leadership do
  @moduledoc """
  The `RadishDB.Raft.Communication.Leadership` module manages the leadership timers and responses
  from followers in the Raft consensus algorithm. It handles the heartbeat and quorum timers,
  tracks follower response times, and determines when to consider followers as unresponsive or unhealthy.

  ## Structure

  The leadership state is represented by a struct with the following fields:

  - `heartbeat_timer`: A reference to the timer used for sending heartbeats to followers.
  - `quorum_timer`: A reference to the timer used for tracking quorum responses.
  - `quorum_timer_started_at`: A timestamp indicating when the quorum timer was started.
  - `follower_responded_times`: A map that tracks the latest timestamps of responses from each follower.

  ## Usage

  This module provides functions to create a new leadership state for a leader, reset timers,
  handle follower responses, and manage the leadership lifecycle.
  """

  alias RadishDB.Raft.Communication.Members
  alias RadishDB.Raft.Types.Config
  alias RadishDB.Raft.Utils.Collections.PidSet
  alias RadishDB.Raft.Utils.{Monotonic, Timer}

  use Croma.Struct, fields: [
    heartbeat_timer:          Croma.Reference,
    quorum_timer:             Croma.Reference,
    quorum_timer_started_at:  Monotonic,

    # %{pid => Monotonic.t} : latest `:leader_timestamp`s each follower responded to
    follower_responded_times: Croma.Map,
  ]

  defun new_for_leader(config :: Config.t) :: t do
    %__MODULE__{
      heartbeat_timer:          start_heartbeat_timer(config),
      quorum_timer:             start_quorum_timer(config),
      quorum_timer_started_at:  Monotonic.milliseconds(),
      follower_responded_times: %{},
    }
  end

  defun reset_heartbeat_timer(%__MODULE__{heartbeat_timer: timer} = l, config :: Config.t) :: t do
    Timer.cancel(timer)
    %__MODULE__{l | heartbeat_timer: start_heartbeat_timer(config)}
  end
  defun reset_quorum_timer(%__MODULE__{quorum_timer: timer} = l, config :: Config.t) :: t do
    Timer.cancel(timer)
    %__MODULE__{l | quorum_timer: start_quorum_timer(config), quorum_timer_started_at: Monotonic.milliseconds()}
  end

  defunp start_heartbeat_timer(%Config{heartbeat_timeout: timeout}) :: reference do
    Timer.make(timeout, :heartbeat_timeout)
  end
  defunp start_quorum_timer(config :: Config.t) :: reference do
    Timer.make(max_election_timeout(config), :cannot_reach_quorum)
  end

  defun follower_responded(%__MODULE__{quorum_timer_started_at: started_at, follower_responded_times: times} = l,
                           %Members{all: all} = members,
                           follower  :: pid,
                           timestamp :: Monotonic.t,
                           config    :: Config.t) :: t do
    follower_pids = PidSet.delete(all, self()) |> PidSet.to_list()
    new_times =
      Map.update(times, follower, timestamp, &max(&1, timestamp))
      |> Map.take(follower_pids) # filter by the actual members, in order not to be disturbed by message from already-removed member
    new_leadership = %__MODULE__{l | follower_responded_times: new_times}
    case quorum_last_reached_at(new_leadership, members) do
      nil        -> new_leadership
      reached_at ->
        if started_at < reached_at do
          reset_quorum_timer(new_leadership, config)
        else
          new_leadership
        end
    end
  end

  defun stop_timers(%__MODULE__{heartbeat_timer: t1, quorum_timer: t2}) :: :ok do
    Timer.cancel(t1)
    Timer.cancel(t2)
  end

  defun unresponsive_followers(%__MODULE__{follower_responded_times: times},
                               members :: Members.t,
                               config :: Config.t) :: [pid] do
    since = Monotonic.milliseconds() - max_election_timeout(config)
    Members.other_members_list(members)
    |> Enum.filter(fn pid ->
      case times[pid] do
        nil -> true
        t   -> t < since
      end
    end)
  end

  defun can_safely_remove?(%__MODULE__{} = l, %Members{all: all} = members, follower :: pid, config :: Config.t) :: boolean do
    unhealthy_followers = unresponsive_followers(l, members, config)
    if follower in unhealthy_followers do
      # unhealthy follower can always be safely removed
      true
    else
      # healthy follower can be removed if remaining members can reach majority
      n_members_after_remove         = PidSet.size(all) - 1
      n_healthy_members_after_remove = n_members_after_remove - length(unhealthy_followers)
      n_healthy_members_after_remove * 2 > n_members_after_remove
    end
  end

  defun remove_follower_response_time_entry(%__MODULE__{follower_responded_times: times} = leadership, follower :: pid) :: t do
    %__MODULE__{leadership | follower_responded_times: Map.delete(times, follower)}
  end

  defun lease_expired?(%__MODULE__{follower_responded_times: times},
                       %Members{all: all},
                       %Config{election_timeout: timeout,
                               election_timeout_clock_drift_margin: margin}) :: boolean do
    n = PidSet.size(all)
    if n <= 1 do
      # 1-member consensus group, leader can make decision by itself
      false
    else
      case quorum_last_reached_at_impl(times, n) do
        nil        -> true
        reached_at -> reached_at + timeout - margin <= Monotonic.milliseconds()
      end
    end
  end

  defunp quorum_last_reached_at(%__MODULE__{follower_responded_times: times}, %Members{all: all}) :: nil | integer do
    n = PidSet.size(all)
    if n <= 1 do
      nil
    else
      quorum_last_reached_at_impl(times, n)
    end
  end

  defunp quorum_last_reached_at_impl(times :: %{pid => Monotonic.t}, n_members :: pos_integer) :: nil | Monotonic.t do
    # To reach quorum we need replies from `n_half_followers` followers.
    n_half_followers = div(n_members, 2) # Note that, based on the caller's logic, `n_members >= 2` and thus `n_half_followers >= 1`
    # Find a timestamp after which `n_half_followers` replies has come in to this leader.
    n_responded = map_size(times)
    if n_responded >= n_half_followers do
      Map.values(times) |> Enum.sort() |> Enum.drop(n_responded - n_half_followers) |> hd()
    else
      # `times` doesn't contain enough items (i.e. right after a leader is elected)
      nil
    end
  end

  defunp max_election_timeout(%Config{election_timeout: t}) :: pos_integer do
    t * 2
  end
end
