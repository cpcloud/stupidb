{ ... }: _: super: {
  tabulate = super.tabulate.overridePythonAttrs (_: {
    preBuild = ''
      export TABULATE_INSTALL="lib-only"
    '';
  });
}
