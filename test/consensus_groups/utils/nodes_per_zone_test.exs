defmodule RadishDB.ConsensusGroups.Utils.NodesPerZoneTest do
  @moduledoc """
  Testing NodesPerZone
  """

  use Croma.TestCase

  alias RadishDB.Utils.Hash

  test "lrw_members/3 should choose member nodes according to hash values" do
    cg = :cg_name
    func = &NodesPerZone.lrw_members/3

    z1 = Hash.calc({"zone1", cg})
    z2 = Hash.calc({"zone2", cg})
    z3 = Hash.calc({"zone3", cg})
    assert z3 < z2 and z2 < z1

    a = Hash.calc({:a, cg})
    b = Hash.calc({:b, cg})
    c = Hash.calc({:c, cg})
    assert c < b and b < a

    d = Hash.calc({:d, cg})
    e = Hash.calc({:e, cg})
    f = Hash.calc({:f, cg})
    assert d < f and f < e

    g = Hash.calc({:g, cg})
    h = Hash.calc({:h, cg})
    i = Hash.calc({:i, cg})
    assert g < i and i < h

    nodes0 = %{}
    assert func.(nodes0, cg, 1) == []
    assert func.(nodes0, cg, 2) == []

    nodes1 = %{"zone1" => [:a, :b, :c]}
    assert func.(nodes1, cg, 1) == [:c]
    assert func.(nodes1, cg, 2) == [:c, :b]
    assert func.(nodes1, cg, 3) == [:c, :b, :a]
    assert func.(nodes1, cg, 4) == [:c, :b, :a]

    nodes2 = %{"zone1" => [:a, :b, :c], "zone2" => [:d, :e, :f]}
    assert func.(nodes2, cg, 1) == [:d]
    assert func.(nodes2, cg, 2) == [:d, :c]
    assert func.(nodes2, cg, 3) == [:d, :c, :b]
    assert func.(nodes2, cg, 4) == [:d, :c, :b, :f]
    assert func.(nodes2, cg, 5) == [:d, :c, :b, :f, :a]
    assert func.(nodes2, cg, 6) == [:d, :c, :b, :f, :a, :e]
    assert func.(nodes2, cg, 7) == [:d, :c, :b, :f, :a, :e]

    nodes3 = %{"zone1" => [:a, :b, :c], "zone2" => [:d, :e, :f], "zone3" => [:g, :h, :i]}
    assert func.(nodes3, cg, 1) == [:d]
    assert func.(nodes3, cg, 2) == [:d, :g]
    assert func.(nodes3, cg, 3) == [:d, :g, :c]
    assert func.(nodes3, cg, 4) == [:d, :g, :c, :b]
    assert func.(nodes3, cg, 5) == [:d, :g, :c, :b, :f]
    assert func.(nodes3, cg, 6) == [:d, :g, :c, :b, :f, :i]
    assert func.(nodes3, cg, 7) == [:d, :g, :c, :b, :f, :i, :a]
    assert func.(nodes3, cg, 8) == [:d, :g, :c, :b, :f, :i, :a, :h]
    assert func.(nodes3, cg, 9) == [:d, :g, :c, :b, :f, :i, :a, :h, :e]
    assert func.(nodes3, cg, 10) == [:d, :g, :c, :b, :f, :i, :a, :h, :e]

    nodes4 = %{"zone1" => [:a, :b], "zone2" => [:c]}
    assert func.(nodes4, cg, 1) == [:c]
    assert func.(nodes4, cg, 2) == [:c, :b]
    assert func.(nodes4, cg, 3) == [:c, :b, :a]
    assert func.(nodes4, cg, 4) == [:c, :b, :a]

    nodes5 = %{"zone1" => [:a], "zone2" => [:b, :c]}
    assert func.(nodes5, cg, 1) == [:c]
    assert func.(nodes5, cg, 2) == [:c, :a]
    assert func.(nodes5, cg, 3) == [:c, :a, :b]
    assert func.(nodes5, cg, 4) == [:c, :a, :b]
  end
end
