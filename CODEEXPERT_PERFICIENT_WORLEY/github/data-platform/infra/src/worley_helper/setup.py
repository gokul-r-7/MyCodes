from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "worley-helper package"
LONG_DESCRIPTION = "worley-helper package"
INSTALL_REQUIRES = [
    "pydantic==2.7.1", 
    "pyyaml==6.0.1", 
    "pydantic-core==2.18.2",
    "aws-lambda-powertools==3.3.0",
    "aws-xray-sdk==2.14.0"
]

# Setting up
setup(
    name="worley-helper",
    python_requires=">=3.9",
    packages=find_packages(exclude=["tests", "tests.*"]),
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    install_requires=INSTALL_REQUIRES,
    version=VERSION,
)
