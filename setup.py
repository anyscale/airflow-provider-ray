from setuptools import find_packages, setup

setup(
    name='airflow-provider-sample',
    description='An Apache Airflow provider for Ray',
    long_description="A longer description of the provider(fill in later).",
    long_description_content_type='text/markdown',
    license='Apache License 2.0',
    version='0.0.1',
    packages=find_packages(),
    zip_safe=False,
    install_requires=['apache-airflow~=2.0'],
    setup_requires=['setuptools', 'wheel'],
    extras_require={},
    python_requires='~=3.6',
)
