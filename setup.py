from setuptools import setup

with open('README.rst', 'r') as f:
    readme = f.read()

setup(
    name='plume',
    py_modules=['plume'],
    version='0.1.0',
    description=(
        'SQLite3 as a document database 🚀'
    ),
    long_description=readme,
    author='Guillaume Paulet',
    author_email='guillaume.paulet@giome.fr',
    license='Public Domain',
    url='https://github.com/ducdetronquito/plume',
    download_url=(
        'https://github.com/ducdetronquito/plume/archive/'
        '0.1.0.tar.gz'
    ),
    tests_require=['mypy', 'pytest'],
    keywords=[
        'TO BE DETERMINED'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: Public Domain',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
