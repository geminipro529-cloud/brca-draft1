# setup.py
# DEFINITIVE VERSION with src Layout and Entry Point

from setuptools import setup, find_packages

setup(
    name="compass_brca",
    version="1.0.0",
    
    # --- V.FINAL FIX: Tell setuptools where the source code is ---
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    
    # This block creates the command-line tool
    entry_points={
        'console_scripts': [
            'run-pipeline = compass_brca.__main__:main',
        ],
    },
)