let
  pkgs = import ./nix;
  poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
    projectDir = ./.;
    editablePackageSources = {
      stupidb = ./stupidb;
    };
  };
in
pkgs.mkShell {
  name = "stupidb";
  shellHook = ''
    ${(import ./pre-commit.nix).pre-commit-check.shellHook}
  '';
  buildInputs = (with pkgs; [ git poetry graphviz gcc ]) ++ [ poetryEnv ];
}
