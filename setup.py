#!/usr/bin/env python

from distutils.core import setup
setup(name='pgcsv',
      version='2.0.0',
      py_modules=['pgcsv'],
      scripts=['scripts/pgcsv', 'scripts/csvclean']
      )

