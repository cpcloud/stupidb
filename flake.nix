{
  description = "The stupidest of all databases";

  inputs = {
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };

    flake-utils.url = "github:numtide/flake-utils";

    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable-small";

    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };

    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
  };

  outputs = { self, flake-utils, gitignore, nixpkgs, poetry2nix, pre-commit-hooks, ... }:
    {
      overlay = nixpkgs.lib.composeManyExtensions [
        poetry2nix.overlay
        gitignore.overlay
        (pkgs: super: {
          prettierTOML = pkgs.writeShellScriptBin "prettier" ''
            ${pkgs.nodePackages.prettier}/bin/prettier \
            --plugin-search-dir "${pkgs.nodePackages.prettier-plugin-toml}/lib" \
            "$@"
          '';
          stupidbDevEnv = pkgs.stupidbDevEnv310;
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

                    projectDir = ./.;
                    src = pkgs.gitignoreSource ./.;

                    buildInputs = [ pkgs.sqlite ];

                    overrides = pkgs.poetry2nix.overrides.withDefaults (
                      import ./poetry-overrides.nix { inherit pkgs; }
                    );

                    checkInputs = with pkgs; [ graphviz-nox imagemagick ];

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
            [ "3.9" "3.10" ]
        )))
      ];
    } // (flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlay ];
        };
        inherit (pkgs) lib;
      in
      rec {
        packages.stupidb39 = pkgs.stupidb39;
        packages.stupidb310 = pkgs.stupidb310;
        packages.stupidb = packages.stupidb310;

        defaultPackage = packages.stupidb;

        checks = {
          pre-commit-check = pre-commit-hooks.lib.${system}.run {
            src = ./.;
            hooks = {
              black.enable = true;
              flake8.enable = true;
              isort.enable = true;
              nix-linter.enable = true;
              nixpkgs-fmt.enable = true;
              shellcheck.enable = true;
              shfmt.enable = true;

              prettier = {
                enable = true;
                entry = lib.mkForce "${pkgs.prettierTOML}/bin/prettier --check";
                types_or = [ "json" "toml" "yaml" ];
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
            git
            graphviz-nox
            imagemagick
            nodejs
            poetry
            prettierTOML
            stupidbDevEnv310
          ];

          shellHook = ''
            ${self.checks.${system}.pre-commit-check.shellHook}
            export PYTHONPATH=$PWD''${PYTHONPATH:+:}$PYTHONPATH
          '';
        };
      }));
}
