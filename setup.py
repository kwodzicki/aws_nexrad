#!/usr/bin/env python
import setuptools
from distutils.util import convert_path

main_ns  = {};
ver_path = convert_path("aws_nexrad/version.py");
with open(ver_path) as ver_file:
  exec(ver_file.read(), main_ns);

setuptools.setup(
  name             = "aws_nexrad",
  description      = "Package for downloading NEXRAD Level 2 data from AWS",
  url              = "https://github.com/kwodzicki/aws_nexrad",
  author           = "Kyle R. Wodzicki",
  author_email     = "krwodzicki@gmail.com",
  version          = main_ns['__version__'],
  packages         = setuptools.find_packages(),
  install_requires = [ "boto3"],
  scripts          = ["bin/aws_nexrad_level2"],
  zip_save         = False,
);
