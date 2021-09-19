{ pkgs, ... }: pyself: pysuper: {
  pytest-randomly = pysuper.pytest-randomly.overrideAttrs (attrs: {
    propagatedBuildInputs = (attrs.propagatedBuildInputs or [ ])
      ++ [ pyself.importlib-metadata ];
  });

  cytoolz = pysuper.cytoolz.overridePythonAttrs (attrs: {
    nativeBuildInputs = (attrs.nativeBuildInputs or [ ])
      ++ [ pkgs.stdenv.cc ];
  });
}
