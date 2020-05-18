from setuptools import setup, find_packages

setup(
    name='data-storage',
    version='1.0.0',
    packages=find_packages(),
    description='The client library for pushing resource to or fetching resource from storage',
    url='https://github.com/rockychen-dpaw/eventhub-client',
    author='Rocky Chen',
    author_email='rocky.chen@dbca.wa.gov.au',
    license='Apache License, Version 2.0',
    zip_safe=False,
    install_requires=[
        'python-dotenv==0.10.3',
        'pytz==2019.3',
        'psycopg2==2.8.4',
        'peewee==3.13.1',
        'dill==0.3.1.1'
    ]
)
