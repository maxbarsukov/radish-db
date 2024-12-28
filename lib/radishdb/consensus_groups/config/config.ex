use Croma

defmodule RadishDB.ConsensusGroups.Config.Config do
  @moduledoc """
  A module that defines application configs
  """

  @default_balancing_interval (if Mix.env() == :test, do:  1_000, else:  60_000)

  @doc """
  `:balancing_interval`
  - Time interval between periodic triggers of workers whose job is to re-balance Raft member processes across the cluster.
  - The actual value used is obtained by
    `Application.get_env(:radish_db, :balancing_interval, #{@default_balancing_interval})`,
    i.e. it defaults to #{div(@default_balancing_interval, 60_000)} minute.
  """
  defun balancing_interval() :: pos_integer do
    Application.get_env(:radish_db, :balancing_interval, @default_balancing_interval)
  end

  @default_leader_pid_cache_refresh_interval 300_000

  @doc """
  `:leader_pid_cache_refresh_interval`
  - Interval time in milliseconds of leader pids cached in each nodes' local ETS tables.
  - The actual value used is obtained by
    `Application.get_env(:radish_db, :leader_pid_cache_refresh_interval, #{@default_leader_pid_cache_refresh_interval})`,
    i.e. it defaults to #{div(@default_leader_pid_cache_refresh_interval, 60_000)} minutes.
  """
  defun leader_pid_cache_refresh_interval() :: pos_integer do
    Application.get_env(
      :radish_db,
      :leader_pid_cache_refresh_interval,
      @default_leader_pid_cache_refresh_interval
    )
  end

  @default_follower_addition_delay 200

  @doc """
  `:follower_addition_delay`
  - Time duration in milliseconds to wait for before spawning a new follower for a consensus group.
    Concurrently spawning multiple followers may lead to race conditions (adding a member can only be done one-by-one).
    Although this race condition can be automatically resolved by retries and thus is basically harmless,
    this configuration item may be useful to reduce useless error logs.
  - The actual value used is obtained by
    `Application.get_env(:radish_db, :follower_addition_delay, #{@default_follower_addition_delay})`.
  """
  defun follower_addition_delay() :: pos_integer do
    Application.get_env(:radish_db, :follower_addition_delay, @default_follower_addition_delay)
  end

  @default_node_purge_failure_time_window (if Mix.env() == :test, do: 30_000, else: 600_000)

  @doc """
  `:node_purge_failure_time_window`
  - A node is considered "unhealthy" if it has been disconnected from the other nodes
    without declaring itself as `inactive` (by calling `RadishDb.ConsensusGroups.Manager.deactivate/0`).
    RadishDb tries to reconnect to unhealthy nodes in order to recover from short-term issues
    such as temporary network failures (see also `:node_purge_reconnect_interval` below).
    To handle longer-term issues, RadishDb automatically removes nodes that remain "unhealthy"
    for this time window (in milliseconds) from the list of participating active nodes.
    After removal, consensus member processes are automatically re-balanced within remaining active nodes.
  - The actual value used is obtained by
    `Application.get_env(:radish_db, :node_purge_failure_time_window, #{@default_node_purge_failure_time_window})`,
    i.e. it defaults to #{div(@default_node_purge_failure_time_window, 60_000)} minutes.
  """
  defun node_purge_failure_time_window() :: pos_integer do
    Application.get_env(
      :radish_db,
      :node_purge_failure_time_window,
      @default_node_purge_failure_time_window
    )
  end

  @default_node_purge_reconnect_interval (if Mix.env() == :test, do:  5_000, else:  60_000)

  @doc """
  `:node_purge_reconnect_interval`
  - Time interval (in milliseconds) of periodic reconnect attempts to disconnected nodes.
  - The actual value used is obtained by
    `Application.get_env(:radish_db, :node_purge_reconnect_interval, #{@default_node_purge_reconnect_interval})`,
    i.e. it defaults to #{div(@default_node_purge_reconnect_interval, 60_000)} minute.
  """
  defun node_purge_reconnect_interval() :: pos_integer do
    Application.get_env(
      :radish_db,
      :node_purge_reconnect_interval,
      @default_node_purge_reconnect_interval
    )
  end

  defun per_member_options() :: nil | module do
    Application.get_env(:radish_db, :per_member_options)
  end

  defun raft_config() :: nil | module do
    Application.get_env(:radish_db, :raft_config)
  end
end
