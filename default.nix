{ python ? "3.8" }:
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
      overrides = pkgs.poetry2nix.overrides.withDefaults (
        import ./poetry-overrides.nix { inherit pkgs; }
      );

      buildInputs = [ graphviz-nox imagemagick_light ];

      checkInputs = [ graphviz-nox imagemagick_light ];

      checkPhase = ''
        runHook preCheck
        pytest --doctest-modules --numprocesses auto
        runHook postCheck
      '';

      pythonImportsCheck = [ "stupidb" ];
    };
in
pkgs.callPackage drv {
  python = pkgs."python${builtins.replaceStrings [ "." ] [ "" ] python}";
}
