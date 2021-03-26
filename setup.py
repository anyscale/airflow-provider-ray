from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

# 'Scale and schedule machine learning and data wrangling workflows using Ray and Airflow together'
setup(
    name='airflow-provider-ray',
    description='An Apache Airflow provider for Ray',
    entry_points='''
        [apache_airflow_provider]
        provider_info=ray_provider.__init__:get_provider_info
    ''',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='Apache License 2.0',
    version='0.1.0.alpha0',
    packages=['ray_provider'],
    zip_safe=False,
    install_requires=[
        'apache-airflow>=2.0',
        'pandas>=1.0.0',
        'filelock>=3.0.0',
        'ray@https://s3-us-west-2.amazonaws.com/ray-wheels/master/fd4ed3acfe2c4c2819d8cd02364d0c5cbc7516ea/ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl#egg=ray'
    ],
    setup_requires=['setuptools', 'wheel'],
    extras_require={},
    author='Rob Deeb',
    author_email='rob@astronomer.io',
    maintainer='Rob Deeb',
    maintainer_email='rob@astronomer.io',
    keywords=['ray','distributed','compute','airflow'],
    python_requires='~=3.7',
)
