import os
import os.path

from setuptools import find_packages
from setuptools import setup


def find_requires():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    requirements = []
    with open(f"{dir_path}/requirements.txt") as reqs:
        requirements = reqs.readlines()
    return requirements


if __name__ == "__main__":
    setup(
        name="pipereport",
        version="0.0.1",
        description="lineage-based ETL/reverse ETL tool",
        packages=find_packages(),
        install_requires=find_requires(),
        include_package_data=True,
        namespace_packages=["pipereport"],
    )
