_: super: {
  mypy = super.mypy.overridePythonAttrs (_: {
    MYPY_USE_MYPYC = false;
  });
}
