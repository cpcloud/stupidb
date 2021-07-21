{ python ? "python37" }:
let
  pkgs = import ./nix;
  drv =
    { poetry2nix
    , python
    , graphviz-nox
    , imagemagick_light
    }:

    poetry2nix.mkPoetryApplication {
      inherit python;

      projectDir = ./.;
      propagatedBuildInputs = [ graphviz-nox imagemagick_light ];
      checkInputs = [ graphviz-nox imagemagick_light ];
      checkPhase = ''
        runHook preCheck
        pytest
        runHook postCheck
      '';
      pythonImportsCheck = [ "stupidb" ];
    };
in
pkgs.callPackage drv {
  python = pkgs.${python};
}
