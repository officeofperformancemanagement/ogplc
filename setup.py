from setuptools import setup
from os import path

this_directory = path.abspath(path.dirname(__file__))

with open(path.join(this_directory, "README.md")) as f:
    long_description = f.read()

setup(
    name="ogplc",
    packages=["ogplc"],
    entry_points={
        "console_scripts": ["ogplc=ogplc.__init__:main"],
    },
    version="0.0.0",
    description="High-Level Database Client for OpenGov Permitting & Licensing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Daniel J. Dufour",
    author_email="daniel.j.dufour@gmail.com",
    url="https://github.com/officeofperformancemanagement/ogplc",
    download_url="https://github.com/officeofperformancemanagement/ogplc/tarball/download",
    keywords=["data", "python"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Operating System :: OS Independent",
    ],
    install_requires=["pyodbc"],
)
