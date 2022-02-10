{ ... }: _: pysuper: {
  tabulate = pysuper.tabulate.overridePythonAttrs (_: {
    TABULATE_INSTALL = "lib-only";
  });
}
