# -*- coding: utf-8 -*-

"""Installation script of the package."""

from pathlib import Path
from setuptools import find_packages, setup

def get_version():
    """Extract the package's version number from the ``VERSION`` file."""
    return (Path(__file__).parent.resolve() / "portfolio_tracking" / "VERSION").read_text(encoding="utf-8").strip()


def get_long_description():
    """Extract README content"""
    return (Path(__file__).parent.resolve() / "README.md").read_text(encoding="utf-8")


setup(
    name="portfolio_tracking",
    version=get_version(),
    author="Clément STUTZ",
    packages=find_packages(),
    install_requires=[
        "yfinance",  # Ajoutez ici toutes les dépendances requises par votre projet
    ],
    extras_require={
            'dev' : ["pytest"]
        },
)
