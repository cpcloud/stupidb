let
  sources = import ./sources.nix;
in
import sources.nixpkgs {
  overlays = [
    (pkgs: _: {
      poetry2nix = import sources.poetry2nix {
        inherit pkgs;
        inherit (pkgs) poetry;
      };
    })
  ];
}
