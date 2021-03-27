from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = [
    'apache-beam[gcp]==2.28.0',
    'pytimeparse',
    'pyyaml'
]

packages = find_packages()
print(packages)

setup(
    name='dataflow-job',
    version='1.1',
    description='Transfer the entities of Datastore to BigQuery.',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages()
)
