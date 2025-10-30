"""""
The setup.py file is an essential part of packaging and distributing
Python projects. It is used by setuptools (or distutills in older Python versions)
to define the configuration of your projects, such as it's metadata, dependencies, and more

"""""

from setuptools import setup, find_packages
from typing import List

def get_requirements():
    """
    This function will return list of requirements

    """
    requirement_lst:List[str]= []
    try:
        with open("requirements.txt", "r") as file:
            #Read lines from the file
            lines= file.readlines()
            #Process each line
            for line in lines:
                requirement = line.strip()
                ## Ignore empty lines and -e.
                if requirement and requirement != "-e .":
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found.")

    return requirement_lst

setup(
    name="NetworkSecurityProject",
    version="0.0.1",
    author="Alanabo Amaegbe",
    author_email="alanaboamaegbe@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements()
)