let
  pkgs = import ./nix;
  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    projectDir = ./.;
    editablePackageSources = {
      stupidb = ./stupidb;
    };
  };
in {
  buildShell = pkgs.mkShell {
    name = "stupidb-build";
    shellHook = ''
      ${(import ./pre-commit.nix).pre-commit-check.shellHook}
    '';
    buildInputs = with pkgs; [ poetry graphviz gcc ];
  };
  devShell = pkgs.mkShell {
    name = "stupidb-dev";
    buildInputs = [ poetryEnv ];
  };
}
