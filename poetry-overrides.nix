{ ... }: pyself: pysuper: {
  pytest-randomly = pysuper.pytest-randomly.overridePythonAttrs (attrs: {
    propagatedBuildInputs = (attrs.propagatedBuildInputs or [ ])
      ++ [ pyself.importlib-metadata ];
  });
}
