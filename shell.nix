let
  pkgs = import ./nix;
  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    projectDir = ./.;
    editablePackageSources = {
      stupidb = ./stupidb;
    };
    overrides = pkgs.poetry2nix.overrides.withDefaults (
      import ./poetry-overlay.nix
    );
  };
  condaShellRun = pkgs.writeShellScriptBin "conda-shell-run" ''
    ${pkgs.conda}/bin/conda-shell -c "$@"
  '';
in
pkgs.mkShell {
  name = "stupidb";
  shellHook = ''
    ${(import ./pre-commit.nix).pre-commit-check.shellHook}
    ${pkgs.conda}/bin/conda-shell -c 'conda-install 2> /dev/null || true'
  '';
  buildInputs = (
    with pkgs; [
      conda
      gcc
      git
      graphviz
      poetry
    ]
  ) ++ [
    poetryEnv
    condaShellRun
  ];
}
