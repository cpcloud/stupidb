{ ... }: pyself: pysuper: {
  black = pysuper.black.overridePythonAttrs (_: {
    dontPreferSetupPy = true;
  });

  tabulate = pysuper.tabulate.overridePythonAttrs (_: {
    TABULATE_INSTALL = "lib-only";
  });
  typing-extensions = pysuper.typing-extensions.overridePythonAttrs (attrs: {
    buildInputs = (attrs.buildInputs or [ ]) ++ [ pyself.flit-core ];
  });
}
