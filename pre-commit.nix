let
  pkgs = import ./nix;
  inherit (pkgs) lib;
  sources = import ./nix/sources.nix;
  pre-commit-hooks = import sources.pre-commit-hooks;
in
{
  pre-commit-check = pre-commit-hooks.run {
    src = ./.;
    hooks = {
      black = {
        enable = true;
        entry = lib.mkForce "black --check";
      };

      isort = {
        enable = true;
        name = "isort";
        language = "python";
        entry = "isort --check";
        types_or = [ "cython" "pyi" "python" ];
      };

      flake8 = {
        enable = true;
        name = "flake8";
        language = "python";
        entry = "flake8";
        types = [ "python" ];
      };

      nix-linter = {
        enable = true;
        excludes = [
          "nix/sources.nix"
        ];
      };
      nixpkgs-fmt.enable = true;

      prettier =
        let
          prettier-toml = pkgs.writeShellScriptBin "prettier-toml" ''
            ${pkgs.nodePackages.prettier}/bin/prettier \
            --plugin-search-dir "${pkgs.nodePackages.prettier-plugin-toml}/lib" \
            --check \
            "$@"
          '';
        in
        {
          enable = true;
          entry = lib.mkForce "${prettier-toml}/bin/prettier-toml";
          types_or = lib.mkForce [ "toml" ];
          types = [ "toml" ];
        };
    };
  };
}
