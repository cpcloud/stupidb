{
  description = "The stupidest of all the databases.";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";

    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable-small";

    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs = { self, nixpkgs, flake-utils, pre-commit-hooks, poetry2nix }:
    {
      overlay = nixpkgs.lib.composeManyExtensions [
        poetry2nix.overlay
        (pkgs: super: {
          prettierTOML = pkgs.writeShellScriptBin "prettier" ''
            ${pkgs.nodePackages.prettier}/bin/prettier \
            --plugin-search-dir "${pkgs.nodePackages.prettier-plugin-toml}/lib" \
            "$@"
          '';
        } // (super.lib.listToAttrs (
          super.lib.concatMap
            (py:
              let
                noDotPy = super.lib.replaceStrings [ "." ] [ "" ] py;
              in
              [
                {
                  name = "stupidb${noDotPy}";
                  value = pkgs.poetry2nix.mkPoetryApplication {
                    python = pkgs."python${noDotPy}";

                    pyproject = ./pyproject.toml;
                    poetrylock = ./poetry.lock;
                    src = pkgs.lib.cleanSource ./.;

                    buildInputs = [ pkgs.sqlite ];

                    overrides = pkgs.poetry2nix.overrides.withDefaults (
                      import ./poetry-overrides.nix { inherit pkgs; }
                    );

                    checkInputs = with pkgs; [
                      graphviz-nox
                      imagemagick_light
                    ];

                    checkPhase = ''
                      runHook preCheck
                      pytest --numprocesses auto
                      runHook postCheck
                    '';

                    pythonImportsCheck = [ "stupidb" ];
                  };
                }
                {
                  name = "stupidbDevEnv${noDotPy}";
                  value = pkgs.poetry2nix.mkPoetryEnv {
                    python = pkgs."python${noDotPy}";
                    projectDir = ./.;
                    overrides = pkgs.poetry2nix.overrides.withDefaults (
                      import ./poetry-overrides.nix { inherit pkgs; }
                    );
                    editablePackageSources = {
                      stupidb = ./stupidb;
                    };
                  };
                }
              ])
            [ "3.7" "3.8" "3.9" "3.10" ]
        )))
      ];
    } // (
      flake-utils.lib.eachDefaultSystem (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ self.overlay ];
          };
          inherit (pkgs) lib;
        in
        rec {
          packages.stupidb37 = pkgs.stupidb37;
          packages.stupidb38 = pkgs.stupidb38;
          packages.stupidb39 = pkgs.stupidb39;
          packages.stupidb310 = pkgs.stupidb310;

          defaultPackage = packages.stupidb310;

          checks = {
            pre-commit-check = pre-commit-hooks.lib.${system}.run {
              src = ./.;
              hooks = {
                nix-linter = {
                  enable = true;
                  entry = lib.mkForce "${pkgs.nix-linter}/bin/nix-linter";
                };

                nixpkgs-fmt = {
                  enable = true;
                  entry = lib.mkForce "${pkgs.nixpkgs-fmt}/bin/nixpkgs-fmt --check";
                };

                shellcheck = {
                  enable = true;
                  entry = "${pkgs.shellcheck}/bin/shellcheck";
                  files = "\\.sh$";
                };

                shfmt = {
                  enable = true;
                  entry = "${pkgs.shfmt}/bin/shfmt -i 2 -sr -d -s -l";
                  files = "\\.sh$";
                };

                prettier = {
                  enable = true;
                  entry = lib.mkForce "${pkgs.prettierTOML}/bin/prettier --check";
                  types_or = [ "json" "toml" "yaml" ];
                };

                black = {
                  enable = true;
                  entry = lib.mkForce "black --check";
                  types = [ "python" ];
                };

                isort = {
                  enable = true;
                  language = "python";
                  entry = lib.mkForce "isort --check";
                  types_or = [ "cython" "pyi" "python" ];
                };

                flake8 = {
                  enable = true;
                  language = "python";
                  entry = "flake8";
                  types = [ "python" ];
                };

                pyupgrade = {
                  enable = true;
                  entry = "pyupgrade --py37-plus";
                  types = [ "python" ];
                };
              };
            };
          };

          devShell = pkgs.mkShell {
            name = "stupidb";
            nativeBuildInputs = with pkgs; [
              commitizen
              git
              graphviz-nox
              imagemagick_light
              nix-linter
              poetry
              prettierTOML
              shellcheck
              shfmt
              stupidbDevEnv310
            ];
            shellHook = self.checks.${system}.pre-commit-check.shellHook;
          };
        }
      )
    );
}
