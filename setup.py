from setuptools import setup

setup(
  name = "Sparkora",
  version = "0.0.1",
  author = "Souvik Pratiher",
  author_email = "spratiher9@gmail.com",
  description = ("Exploratory data analysis toolkit for Pyspark"),
  license = "GPL-3.0 License",
  keywords = "exploratory data analysis",
  install_requires = [
    "pyspark>=3.1.0",
    "IPython",
  ],
  packages = ['Sparkora']
)