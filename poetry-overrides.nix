{ ... }: pyself: pysuper: {
  pytest-randomly = pysuper.pytest-randomly.overrideAttrs (attrs: {
    propagatedBuildInputs = (attrs.propagatedBuildInputs or [ ])
      ++ [ pyself.importlib-metadata ];
  });
}
