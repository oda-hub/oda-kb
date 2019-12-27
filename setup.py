from setuptools import setup
import ast
import sys

setup_requires = ['setuptools >= 30.3.0', 'setuptools-git-version']

if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires.append('pytest-runner')


setup(description="oda-sparql",
      long_description=open('README.md').read(),
      version=open('VERSION').read().strip(),
      setup_requires=setup_requires)
