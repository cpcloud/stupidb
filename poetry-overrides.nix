{ ... }: _: pysuper: {
  black = pysuper.black.overridePythonAttrs (_: {
    dontPreferSetupPy = true;
  });
}
