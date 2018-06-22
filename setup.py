#!/usr/bin/env python
#
# Setup prog for libfactory
#
#

import sys
import re
from setuptools import setup
import time


def choose_data_file_locations():
    local_install = False

    if '--user' in sys.argv:
        local_install = True
    elif any( [ re.match('--home(=|\s)', arg) for arg in sys.argv] ):
        local_install = True
    elif any( [ re.match('--prefix(=|\s)', arg) for arg in sys.argv] ):
        local_install = True

    if local_install:
        return home_data_files
    else:
        return rpm_data_files

current_time = time.gmtime()
#release_version = "{0}.{1:0>2}.{2:0>2}".format(current_time.tm_year, current_time.tm_mon, current_time.tm_mday)
release_version='1.0.0'

scripts   = []
etc_files = []

rpm_data_files  = []
home_data_files = []
data_files      = choose_data_file_locations()

# ===========================================================

# setup for distutils
print(scripts)
setup(
    name="libfactory",
    version=release_version,
    description='libfactory package',
    long_description='''This package contains libfactory''',
    license='GPL',
    author='Jose Caballero, John Hover',
    author_email='jcaballero@bnl.gov, jhover@bnl.gov',
    maintainer='Jose Caballero, John Hover',
    maintainer_email='jcaballero@bnl.gov, jhover@bnl.gov',
    url='https://github.com/bnl-sdcc/libfactory',
    packages=['libfactory'],
    scripts=scripts,
    data_files=data_files,
    install_requires=[]
)


