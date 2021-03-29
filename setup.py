from setuptools import setup, find_packages
import os

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name='airflow-provider-ray',
    description='An Apache Airflow provider for Ray',
    entry_points={
        "apache_airflow_provider": [
          "provider_info=ray_provider.__init__:get_provider_info"
        ]
    },
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='Apache License 2.0',
    version='0.1.0.alpha0',
    packages=find_packages(include=['*']),
    zip_safe=False,
    install_requires=[
        'apache-airflow>=2.0',
        'pandas>=1.0.0',
        'filelock>=3.0.0',
        'ray==2.0.0.dev0'
    ],
    setup_requires=['setuptools', 'wheel'],
    extras_require={},
    author='Rob Deeb',
    author_email='rob@astronomer.io',
    maintainer='Rob Deeb',
    maintainer_email='rob@astronomer.io',
    keywords=['ray','distributed','compute','airflow'],
    python_requires='~=3.7',
    include_package_data=True
)
