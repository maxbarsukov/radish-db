<img align="right" src="https://raw.githubusercontent.com/maxbarsukov/radish-db/refs/heads/master/docs/assets/logo.png" alt="RadishDB logo" width="365">

# RadishDB

> Yet Another Key–Value Database

![GitHub Release](https://img.shields.io/github/v/release/maxbarsukov/radish-db)
![GitHub License](https://img.shields.io/github/license/maxbarsukov/radish-db)
![GitHub repo size](https://img.shields.io/github/repo-size/maxbarsukov/radish-db) \
[![Elixir](https://github.com/maxbarsukov/radish-db/actions/workflows/elixir.yml/badge.svg?branch=master)](https://github.com/maxbarsukov/radish-db/actions/workflows/elixir.yml)
[![Markdown](https://github.com/maxbarsukov/radish-db/actions/workflows/markdown.yml/badge.svg?branch=master)](https://github.com/maxbarsukov/radish-db/actions/workflows/markdown.yml)
[![Coverage Status](https://coveralls.io/repos/github/maxbarsukov/radish-db/badge.svg?branch=master)](https://coveralls.io/github/maxbarsukov/radish-db?branch=master)

## What is RadishDB?

RadishDB is an in-memory distributed key-value data store that chooses Consistency over Availability using own implementation of [Raft Consensus Algorithm](https://raft.github.io/).

## Table of contents

1. [Updates](#updates)
2. [Getting Started](#getting-started)
   1. [Pre-reqs](#pre-reqs)
   2. [Building and Running](#run)
3. [Testing](#testing)
4. [Linting](#linting)
5. [Contributing](#contributing)
6. [Code of Conduct](#code-of-conduct)
7. [Get in touch!](#get-in-touch)
8. [Security](#security)
9. [Useful Links](#useful-links)
10. [License](#license)

---

## Updates <a name="updates"></a>

<strong>🎉 v0.1.0 has been released!</strong>
<details open>
  <summary><b>🔔 Dec. 28, 2024 (v0.1.0)</b></summary>

  > - Implement [**RAFT Consensus Algorithm**](https://raft.github.io/raft.pdf);
  > - Basic distributed key-value store.

</details>

## 🚀 Getting Started <a name="getting-started"></a>

### Pre-reqs <a name="pre-reqs"></a>

- Make sure you have [`git`](https://git-scm.com/) installed.
- Erlang and Elixir (we recommend using [`asdf`](https://asdf-vm.com/) to manage versions).

### Building and Running <a name="run"></a>

Clone the repository:

```bash
git clone git@github.com:maxbarsukov/radish-db.git
cd radish-db
```

Install dependencies:

```bash
mix deps.get
```

Compile the project:

```bash
mix compile
```

Start the application:

```bash
$ iex --sname 1 -S mix
iex(1@laptop)>
```

#### Example

Let's assume we have a 4-node Erlang cluster:

```bash
$ iex --sname 1 -S mix
iex(1@laptop)>

$ iex --sname 2 -S mix
iex(2@laptop)> Node.connect(:"1@laptop")

$ iex --sname 3 -S mix
iex(3@laptop)> Node.connect(:"1@laptop")

$ iex --sname 4 -S mix
iex(4@laptop)> Node.connect(:"1@laptop")
```

Load the following module, which implements the `RadishDB.Raft.StateMachine.Statable` behavior on all cluster nodes:

```elixir
  defmodule JustAnInt do
    @behaviour RadishDB.Raft.StateMachine.Statable
    def new, do: 0
    def command(i, {:set, j}), do: {i, j}
    def command(i, :inc), do: {i, i + 1}
    def query(i, :get), do: i
  end
```

Call `RadishDB.ConsensusGroups.GroupApplication.activate/1` on all nodes:

```elixir
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.activate("zone1")

iex(2@laptop)> RadishDB.ConsensusGroups.GroupApplication.activate("zone2")

iex(3@laptop)> RadishDB.ConsensusGroups.GroupApplication.activate("zone1")

iex(4@laptop)> RadishDB.ConsensusGroups.GroupApplication.activate("zone2")
```

Than create 5 consensus groups, each of which replicates an integer and has 3 consensus members:

```elixir
iex(1@laptop)> config = RadishDB.Raft.Node.make_config(JustAnInt)
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.add_consensus_group(:consensus1, 3, config)
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.add_consensus_group(:consensus2, 3, config)
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.add_consensus_group(:consensus3, 3, config)
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.add_consensus_group(:consensus4, 3, config)
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.add_consensus_group(:consensus5, 3, config)
```

Now we can execute a query/command from any cluster node:

```elixir
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.query(:consensus1, :get)
{:ok, 0}

iex(2@laptop)> RadishDB.ConsensusGroups.GroupApplication.command(:consensus1, :inc)
{:ok, 0}

iex(3@laptop)> RadishDB.ConsensusGroups.GroupApplication.query(:consensus1, :get)
{:ok, 1}
```
Activation/deactivation of a node in a cluster triggers a rebalancing of the consensus member processes.

A 3-nodes consensus group continues to operate if one of the members dies (even without deactivation):

```elixir
iex(3@laptop)> :gen_statem.stop(:baz)
iex(1@laptop)> RadishDB.ConsensusGroups.GroupApplication.query(:consensus1, :get)
{:ok, 1}
```

## ✅&ensp;Testing <a name="testing"></a>

Run tests with:
```bash
mix test
```

For detailed test output:
```bash
mix test --trace
```

Test coverage is tracked via [Coveralls](https://coveralls.io/github/maxbarsukov/radish-db?branch=master) and shown in the project badges.

## 🎨&ensp;Linting <a name="linting"></a>

We use multiple linting tools:

```bash
# Format code
mix format

# Run Credo for code analysis
mix credo

# Run Dialyzer for type checking
mix dialyzer

# Run all checks (format, credo, dialyzer)
mix check
```

Linting is also automatically run on every commit via GitHub Actions.

---

## 🤝&ensp;Contributing <a name="contributing"></a>

Need help? See [`SUPPORT.md`](./SUPPORT.md).

Hey! We're glad you're thinking about contributing to **RadishDB**! Feel free to pick an issue labeled as `good first issue` and ask any question you need. Some points might not be clear, and we are available to help you!

Bug reports and pull requests are welcome on GitHub at https://github.com/maxbarsukov/radish-db.

Before creating your PR, we strongly encourage you to read the repository's corresponding [`CONTRIBUTING.md`](https://github.com/maxbarsukov/radish-db/blob/master/.github/CONTRIBUTING.md) or otherwise the "Contributing" section of the [`README.md`](https://github.com/maxbarsukov/radish-db/blob/master/README.md).

## ⚖️&ensp;Code of Conduct <a name="code-of-conduct"></a>

This project is intended to be a safe, welcoming space for collaboration, and everyone interacting in the **RadishDB** project's codebases, issue trackers, chat rooms and mailing lists is expected to adhere to the [code of conduct](https://github.com/maxbarsukov/radish-db/blob/master/.github/CODE_OF_CONDUCT.md).

## 📫&ensp;Get in touch! <a name="get-in-touch"></a>

💌 Want to make a suggestion or give feedback? Here are some of the channels where you can reach us:

- Found a bug? [Open an issue]((https://github.com/maxbarsukov/radish-db/issues)) in the repository!
- Want to be part of our Telegram community? We invite you to join our [RadishDB Community Chat](https://t.me/radishdb), where you can find support from our team and the community, but where you can also share your projects or just talk about random stuff with other members of the RadishDB community 😁!

## 🛡️&ensp;Security <a name="security"></a>

**RadishDB** takes the security of our software products and services seriously. If you believe you have found a security vulnerability in any RadishDB-owned repository, please report it to us as described in our [security policy](https://github.com/maxbarsukov/radish-db/security/policy).

---

## 🌐&ensp;Useful Links <a name="useful-links"></a>

| Link | Description |
| --- | --- |
| [raft.github.io](https://raft.github.io/) | Raft Consensus Algorithm |
| [raft.github.io/raft.pdf](https://raft.github.io/raft.pdf) | *Original Paper:* In Search of an Understandable Consensus Algorithm (Extended Version) |
| [habr.com/ru/articles/469999/](https://habr.com/ru/companies/dododev/articles/469999/) | How servers negotiate with each other: Raft distributed consensus algorithm |
| [thesecretlivesofdata.com/raft/](https://thesecretlivesofdata.com/raft/) <br> [deniz.co/raft-consensus/](https://deniz.co/raft-consensus/) | Interactive Raft visualizations |
| [erlang.org/doc/system/distributed.html](https://www.erlang.org/doc/system/distributed.html) | About distributed Erlang systems |

## 🪪&ensp;License <a name="license"></a>

The project is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

**Leave a star :star: if you find this project useful.**

---

*<p align="center">This project is published under [MIT](LICENSE).<br>A [maxbarsukov](https://github.com/maxbarsukov) project.<br>- :tada: -</p>*
