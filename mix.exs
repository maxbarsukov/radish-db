defmodule RadishDB.MixProject do
  @moduledoc """
  Settings for RadishDB mix project.
  """

  use Mix.Project

  @github_url "https://github.com/maxbarsukov/radish-db"
  @version "version" |> File.read!() |> String.trim()

  def project do
    [
      app: :radishdb,
      elixir: "~> 1.17",
      # escript: escript(),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: Mix.compilers() ++ [:croma],
      preferred_cli_env: [
        # Test coverage
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.cobertura": :test
      ],
      test_coverage: [tool: ExCoveralls],
      deps: deps(),
      aliases: aliases(),

      # Docs
      name: "RadishRB",
      source_url: @github_url,
      homepage_url: @github_url,

      # Hex
      description: description(),
      package: package(),
      version: @version
    ]
  end

  # defp escript do
  #   [
  #     main_module: RadishDB.Main,
  #     path: "./out/radishdb",
  #     name: "radishdb"
  #   ]
  # end

  def application do
    [
      applications: [:croma, :logger],
      # mod: {RadishDB.Main, []}
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Types
      {:croma, "~> 0.10"},

      # Testing
      {:quixir, "~> 0.9", only: :test},

      # Test coverage
      {:excoveralls, "~> 0.18", only: :test},

      # Linting & formatting
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16.0", only: [:dev], runtime: false}
    ]
  end

  def aliases do
    []
  end

  def description do
    "RadishDB is an in-memory distributed key-value data store"
  end

  def package do
    [
      files: ["lib", "mix.exs", "LICENSE*", "README*", "version"],
      maintainers: ["Max Barsukov <maxbarsukov@bk.ru>"],
      licenses: ["MIT"],
      links: %{"GitHub" => @github_url}
    ]
  end
end
