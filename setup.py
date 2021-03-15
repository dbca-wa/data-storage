from setuptools import setup, find_packages

setup(
    name='data-storage',
    version='3.9',
    packages=find_packages(),
    description='The client library for pushing resource to or fetching resource from storage',
    url='https://github.com/dbca-wa/data-storage',
    author='Rocky Chen',
    author_email='rocky.chen@dbca.wa.gov.au',
    license='Apache License, Version 2.0',
    zip_safe=False,
    install_requires=[
        'pytz',
        'azure-storage-blob==12.3.1'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
