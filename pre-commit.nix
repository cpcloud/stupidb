let
  pkgs = import ./nix;
  sources = import ./nix/sources.nix;
  pre-commit-hooks = import sources.pre-commit-hooks;
in
{
  pre-commit-check = pre-commit-hooks.run {
    src = ./.;
    hooks = {
      black = {
        enable = true;
        entry = pkgs.lib.mkForce "black";
      };

      isort = {
        enable = true;
        name = "isort";
        language = "python";
        entry = "isort";
        types_or = [ "cython" "pyi" "python" ];
      };

      flake8 = {
        enable = true;
        name = "flake8";
        language = "python";
        entry = "flake8";
        types = [ "python" ];
      };
    };
  };
}
