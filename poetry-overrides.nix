{ ... }: _: pysuper: {
  black = pysuper.black.overridePythonAttrs (_: {
    dontPreferSetupPy = true;
  });

  tabulate = pysuper.tabulate.overridePythonAttrs (_: {
    TABULATE_INSTALL = "lib-only";
  });
}
