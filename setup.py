from setuptools import setup, find_packages
import os

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="airflow-provider-ray",
    description="An Apache Airflow provider for Ray",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=ray_provider.__init__:get_provider_info"
        ]
    },
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache License 2.0",
    version="0.2.0-rc.2",
    packages=find_packages(include=["*"]),
    zip_safe=False,
    install_requires=["apache-airflow>=2.0", "pandas>=1.0.0", "filelock>=3.0.0", "ray"],
    setup_requires=["setuptools", "wheel"],
    extras_require={},
    author="Rob Deeb, Richard Liaw, Daniel Imberman, Pete DeJoy",
    author_email="rob@astronomer.io, daniel@astronomer.io, rliaw@anyscale.com, pete@astronomer.io",
    maintainer="Rob Deeb, Richard Liaw, Daniel Imberman, Pete DeJoy",
    maintainer_email="rob@astronomer.io, daniel@astronomer.io, rliaw@anyscale.com, pete@astronomer.io",
    keywords=["ray", "distributed", "compute", "airflow"],
    python_requires=">=3.7",
    include_package_data=True,
)
