from setuptools import setup, find_packages

DESCRIPTION = "A MongoDB object mapper inspired by Django models and SQLAlchemy's pythonic DSL."

with open('README') as f:
    LONG_DESCRIPTION = f.read()

VERSION = '0.1.0'

CLASSIFIERS = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(
    name='conjure',
    version=VERSION,
    packages=find_packages(),
    author='Stanislav Vishnevskiy',
    author_email='vishnevskiy@gmail.com',
    url='https://github.com/vishnevskiy/conjure',
    license='MIT',
    include_package_data=True,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    install_requires=[
        'pymongo>=2.7.2',
    ],
    platforms=['any'],
    classifiers=CLASSIFIERS,
    test_suite='tests',
)
