{ python ? "3.9" }:
let
  pkgs = import ./nix;
  inherit (pkgs) lib;
  prettier = with pkgs; writeShellScriptBin "prettier" ''
    ${nodePackages.prettier}/bin/prettier \
    --plugin-search-dir "${nodePackages.prettier-plugin-toml}/lib" \
    "$@"
  '';
  mkPoetryEnv = python: pkgs.poetry2nix.mkPoetryEnv {
    inherit python;
    pyproject = ./pyproject.toml;
    poetrylock = ./poetry.lock;
    editablePackageSources = {
      stupidb = ./stupidb;
    };
    overrides = pkgs.poetry2nix.overrides.withDefaults (
      import ./poetry-overrides.nix { }
    );
  };
  name = "python${builtins.replaceStrings [ "." ] [ "" ] python}";
in
pkgs.mkShell {
  name = "stupidb-dev-${name}";
  shellHook = ''
    ${(import ./pre-commit.nix).pre-commit-check.shellHook}
  '';
  buildInputs = (
    with pkgs; [
      fd
      gcc
      git
      gnumake
      graphviz-nox
      imagemagick_light
      niv
      nix-linter
      poetry
    ]
  ) ++ [
    (mkPoetryEnv pkgs.${name})
    prettier
  ];
}
