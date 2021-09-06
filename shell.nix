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

  pythonVersions = [ "3.7" "3.8" "3.9" ];
in
lib.listToAttrs
  (map
    (name: {
      inherit name;
      value = pkgs.mkShell {
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
      };
    })
    (map
      (version: "python${builtins.replaceStrings [ "." ] [ "" ] version}")
      pythonVersions))
