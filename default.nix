{ python ? "python37" }:
let
  pkgs = import ./nix;
  drv =
    { poetry2nix
    , python
    , graphviz
    , lib
    , stdenv
    }:

    poetry2nix.mkPoetryApplication {
      inherit python;

      projectDir = ./.;
      propagatedBuildInputs = [ graphviz ];
      checkInputs = [ graphviz ];
      checkPhase = ''
        runHook preCheck
        pytest ${lib.optionalString stdenv.isDarwin "--ignore=stupidb/tests/test_animate.py"}
        runHook postCheck
      '';
      pythonImportsCheck = [ "stupidb" ];
    };
in
pkgs.callPackage drv {
  python = pkgs.${python};
}
