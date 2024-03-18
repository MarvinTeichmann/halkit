#!/usr/bin/env python
import os
from setuptools import setup, find_packages

rfile = "requirements.txt"
if os.path.isfile(rfile):
    with open(rfile) as fp:
        install_requires = fp.read().splitlines()


setup(
    name="FlinksterAnalytix",
    version="0.1",
    description=(
        "PFlinksterAnalytix: A Python toolkit for data science with DB Flinkster Carsharing data."
        "This library simplifies data ingestion, cleaning, exploratory data analysis (EDA), "
        "and provides functionality for advanced insights, all based on data from the HAL platform "
        "(hal-prod.service.dbrent.net). Ideal for data scientists, researchers, "
        "analysts working looking to work with Flinkster HAL data."
    ),
    author="Marvin Teichmann",
    author_email="marvin.teichmann@googlemail.com",
    packages=find_packages(),
    package_data={"": ["*.lst", "*.json", "*.csv"]},
)
