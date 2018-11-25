#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from typing import List

from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements: List[str] = []

setup_requirements = ["pytest-runner"]

test_requirements = ["pytest"]

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
    description="A really slow database",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="stupidb",
    name="stupidb",
    packages=find_packages(include=["stupidb"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/cpcloud/stupidb",
    version="0.1.0",
    zip_safe=False,
    python_requires=">=3.6",
)
