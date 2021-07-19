let
  pkgs = import ./nix;
  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    projectDir = ./.;
    editablePackageSources = {
      stupidb = ./stupidb;
    };
  };
  condaShellRun = pkgs.writeShellScriptBin "conda-shell-run" ''
    ${pkgs.conda}/bin/conda-shell -c "$@"
  '';
in
{
  conda = pkgs.mkShell {
    name = "stupidb-conda";
    buildInputs = (
      with pkgs; [
        conda
        git
        poetry
      ]
    ) ++ [
      condaShellRun
    ];
  };

  dev = pkgs.mkShell {
    name = "stupidb-dev";
    shellHook = ''
      ${(import ./pre-commit.nix).pre-commit-check.shellHook}
      ${pkgs.conda}/bin/conda-shell -c 'conda-install 2> /dev/null || true'
    '';
    buildInputs = (
      with pkgs; [
        gcc
        git
        graphviz
        poetry
      ]
    ) ++ [
      poetryEnv
    ];
  };
}
