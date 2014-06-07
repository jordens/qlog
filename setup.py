#!/usr/bin/python

from setuptools import setup, find_packages

setup(
        name = "qlog",
        version = "0.0+dev",
        author = "Robert Jordens",
        author_email = "jordens@gmail.com",
        url = "http://github.com/nist-ionstorage/qlog",
        description = "Logging and exploration framework for lab/sensor data",
        long_description = """""",
        license = "GPLv3+",
        install_requires = [
            "Flask", "Flask-RESTful", "SQLAlchemy", "bokeh", "docopt",
            "pandas", "requests", "nose"],
        extras_require = {
            },
        dependency_links = [],
        packages = find_packages(),
        namespace_packages = [],
        test_suite = "nose.collector",
        include_package_data = True,
        )
