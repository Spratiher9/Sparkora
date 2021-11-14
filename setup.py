# Author: Souvik Pratiher
# Project: Sparkora v0.0.1


import os
from setuptools import setup

if os.path.isfile('README.md'):
    with open('README.md', encoding='utf-8') as readme:
        bdescription = readme.read()
else:
    bdescription = "EDA Toolkit for Apache Spark based workflows"

setup(
    name="Sparkora",
    version="0.0.1",
    author="Souvik Pratiher",
    author_email="spratiher9@gmail.com",
    description="Exploratory data analysis toolkit for Pyspark",
    license="GPL-3.0 License",
    keywords=["exploratory data analysis", "EDA", "pyspark", "preprocessing"],
    long_description=bdescription,
    long_description_content_type='text/markdown',
    install_requires=[
        "pyspark>=3.1.0",
        "IPython",
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Topic :: Software Development :: Build Tools',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    packages=['Sparkora']
)
