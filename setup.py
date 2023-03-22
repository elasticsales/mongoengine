import os
import sys
from setuptools import setup, find_packages

# Hack to silence atexit traceback in newer python versions
try:
    import multiprocessing
except ImportError:
    pass

DESCRIPTION = 'MongoEngine is a Python Object-Document ' + \
'Mapper for working with MongoDB.'
LONG_DESCRIPTION = None
try:
    LONG_DESCRIPTION = open('README.rst').read()
except:
    pass


def get_version(version_tuple):
    if not isinstance(version_tuple[-1], int):
        return '.'.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))

# Dirty hack to get version number from monogengine/__init__.py - we can't
# import it as it depends on PyMongo and PyMongo isn't installed until this
# file is read
init = os.path.join(os.path.dirname(__file__), 'mongoengine', '__init__.py')
version_line = list([l for l in open(init) if l.startswith('VERSION')])[0]

VERSION = get_version(eval(version_line.split('=')[-1]))
print(VERSION)

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.1",
    "Programming Language :: Python :: 3.2",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: Implementation :: CPython",
    'Topic :: Database',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

extra_opts = {"packages": find_packages(exclude=["tests", "tests.*"])}

assert sys.version_info[0] == 3

setup(name='mongoengine',
      version=VERSION,
      author='Harry Marr',
      author_email='harry.marr@{nospam}gmail.com',
      maintainer="Ross Lawley",
      maintainer_email="ross.lawley@{nospam}gmail.com",
      url='http://mongoengine.org/',
      download_url='https://github.com/MongoEngine/mongoengine/tarball/master',
      license='MIT',
      include_package_data=True,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      platforms=['any'],
      classifiers=CLASSIFIERS,
      install_requires=['pymongo>=3.0,<3.14'],
      test_suite='nose.collector',
      **extra_opts
)
