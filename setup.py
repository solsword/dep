from distutils.core import setup

import m2r

with open("README") as fin:
  long_desc = m2r.convert(fin.read())

setup(
  name="quiche",
  version="0.3.0",
  url="https://github.com/solsword/quiche",
  description="Make-like system for caching Python function results. Compatible with keras.",
  author="Peter Mawhorter",
  author_email="pmawhorter@gmail.com",
  packages=["quiche"],
  license="Mozilla Public License Version 2.0",
  long_description=long_desc
)
