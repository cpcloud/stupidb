let
  inherit (import ./nix) lib;
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
        types = [ "python" ];
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
        entry = lib.mkForce "nix-linter";
        excludes = [
          "nix/sources.nix"
        ];
      };

      nixpkgs-fmt.enable = true;

      prettier = {
        enable = true;
        entry = lib.mkForce "prettier --check";
        types_or = lib.mkForce [ "toml" "yaml" "json" ];
      };
    };
  };
}
