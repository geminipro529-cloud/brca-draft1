# D:/breast_cancer_project_root/setup.py
from setuptools import setup, find_packages

setup(
    name="compass_brca",
    version="0.1.0",
    # This now correctly finds the 'compass_brca' package directory
    packages=find_packages(where="."),
)