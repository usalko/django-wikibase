from setuptools import setup, find_packages

with open('README.rst') as readme:
    __doc__ = readme.read()


# Provided as an attribute, so you can append to these instead
# of replicating them:
standard_exclude = ('*.py', '*.pyc', '*$py.class', '*~', '.*', '*.bak', '*.orig')
standard_exclude_directories = ('.*', 'CVS', '_darcs', './build', './dist', 'EGG-INFO', '*.egg-info')


# Dynamically calculate the version based on wikibase.VERSION.
version = __import__('wikibase').get_version()

setup(
    name='django-wikibase',
    version=version,
    description='Wikibase backend for Django web framework',
    long_description=__doc__,
    license='BSD',
    author='Ivan Usalko',
    author_email='iusalko@eu.spb.ru',
    url='https://gitlab.com/mast_pandan/mediawiki/-/tree/phytonium/django-wikibase',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Database',
        'Topic :: Internet :: WWW/HTTP',
    ],
    zip_safe=False,
    install_requires=[],
)
