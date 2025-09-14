# setup.py
from setuptools import setup, find_packages

setup(
    name="compass_brca",
    version="1.0.0",
    package_dir={"": "src"},  # Tells setuptools that packages are in the 'src' directory
    packages=find_packages(where="src"),  # Tells find_packages to look for packages in 'src'
    entry_points={
        'console_scripts': [
            'run-pipeline = compass_brca.__main__:main',
        ],
    },
)