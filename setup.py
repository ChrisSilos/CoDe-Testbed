from setuptools import setup, find_packages

setup(
    name='river_node_progress',
    version='1.0',
    install_requires=[
        'paho-mqtt',
        'requests',
        'pandas',
        'datetime',
        'ntplib',
        'scipy'
    ]
)
