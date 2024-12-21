use Croma

defmodule RadishDB.Raft.Types.Config do
  @moduledoc """
  Type for RadishDB Raft implementation config.
  """

  use Croma.Struct,
    fields: [
      data_module: Croma.Atom,
      leader_hook_module: Croma.Atom,
      communication_module: Croma.Atom,

      # minimum value; actual timeout is randomly picked from `election_timeout .. 2 * election_timeout`
      election_timeout: Croma.PosInteger,
      heartbeat_timeout: Croma.PosInteger,
      election_timeout_clock_drift_margin: Croma.PosInteger,
      max_retained_command_results: Croma.PosInteger
    ]
end
