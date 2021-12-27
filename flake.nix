{
  description = "The stupidest of all databases";

  inputs = {
    flake-utils = {
      url = "github:numtide/flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
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

  outputs =
    { self
    , flake-utils
    , gitignore
    , nixpkgs
    , poetry2nix
    , pre-commit-hooks
    }:
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
            [ "3.7" "3.8" "3.9" "3.10" ]
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
      packages.stupidb37 = pkgs.stupidb37;
      packages.stupidb38 = pkgs.stupidb38;
      packages.stupidb39 = pkgs.stupidb39;
      packages.stupidb310 = pkgs.stupidb310;
      packages.stupidb = packages.stupidb310;

      defaultPackage = packages.stupidb;

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
              entry = lib.mkForce "${pkgs.stupidbDevEnv310}/bin/black --check";
              types = [ "python" ];
            };

            isort = {
              enable = true;
              entry = lib.mkForce "${pkgs.stupidbDevEnv310}/bin/isort --check";
              types_or = [ "pyi" "python" ];
            };

            flake8 = {
              enable = true;
              entry = "${pkgs.stupidbDevEnv310}/bin/flake8";
              types = [ "python" ];
            };

            pyupgrade = {
              enable = true;
              entry = "${pkgs.stupidbDevEnv310}/bin/pyupgrade --py37-plus";
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
          imagemagick
          nix-linter
          poetry
          prettierTOML
          shellcheck
          shfmt
          stupidbDevEnv310
        ];
        shellHook = self.checks.${system}.pre-commit-check.shellHook;
      };
    }));
}
