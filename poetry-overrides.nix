{ ... }: pyself: pysuper: {
  tabulate = pysuper.tabulate.overridePythonAttrs (_: {
    TABULATE_INSTALL = "lib-only";
  });

  isort = pysuper.isort.overridePythonAttrs (attrs: {
    nativeBuildInputs = (attrs.nativeBuildInputs or [ ]) ++ [ pyself.poetry ];
  });
}
