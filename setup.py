from setuptools import setup, find_packages

version = '0.0'

setup(
    name='osmshp',
    version=version,
    description="",
    long_description="""""",
    classifiers=[],
    keywords='',
    author='Aleksandr Dezhin',
    author_email='me@dezhin.net',
    url='',
    license='MIT License',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'pyyaml',
        'psycopg2',
        'requests',
        'sqlalchemy',
        'geoalchemy',
    ],
    entry_points={
        'console_scripts': [ 'osmshp = osmshp.script:main', ],
    },
)
