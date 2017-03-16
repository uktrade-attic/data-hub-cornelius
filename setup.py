#!/usr/bin/env python
import setuptools


SETUP_KWARGS = {
    'name': 'cornelius',
    'version': '1.0',
    'description': "Utilities",
    'packages': setuptools.find_packages(),
}

if __name__ == '__main__':
    setuptools.setup(**SETUP_KWARGS)
