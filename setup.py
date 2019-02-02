#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import pathlib

from setuptools import find_packages, setup

readme = pathlib.Path("README.rst").read_text()
history = pathlib.Path("HISTORY.rst").read_text()

requirements = (
    pathlib.Path("requirements_dev.txt").read_text().strip().splitlines()
)

setup(
    author="Phillip Cloud",
    author_email="cpcloud@gmail.com",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    description="A really stupid database",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description="{}\n\n{}".format(readme, history),
    include_package_data=True,
    keywords="stupidb",
    name="stupidb",
    packages=find_packages(include=["stupidb"]),
    url="https://github.com/cpcloud/stupidb",
    version="0.1.0",
    zip_safe=False,
    python_requires=">=3.6",
)
