{ python ? "python37" }:
let
  pkgs = import ./nix;
  drv =
    { poetry2nix
    , python
    , graphviz
    }:

    poetry2nix.mkPoetryApplication {
      inherit python;

      projectDir = ./.;
      propagatedBuildInputs = [ graphviz ];
      checkInputs = [ graphviz ];
      checkPhase = ''
        runHook preCheck
        pytest
        runHook postCheck
      '';
    };
in
pkgs.callPackage drv {
  python = pkgs.${python};
}
