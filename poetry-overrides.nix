{ pkgs, ... }: pyself: pysuper: {
  pytest-randomly = pysuper.pytest-randomly.overrideAttrs (attrs: {
    propagatedBuildInputs = (attrs.propagatedBuildInputs or [ ])
      ++ [ pyself.importlib-metadata ];
    postPatch = "";
  });

  cytoolz = pysuper.cytoolz.overridePythonAttrs (attrs: {
    nativeBuildInputs = (attrs.nativeBuildInputs or [ ])
      ++ [ pkgs.stdenv.cc ];
  });

  black = pysuper.black.overridePythonAttrs (_: {
    dontPreferSetupPy = true;
  });
}
