
# from setuptools import setup, find_packages

# setup(
#     #this will be the package name you will see, e.g. the output of 'conda list' in anaconda prompt
#     name = 'malib',
#     #some version number you may wish to add - increment this after every update
#     version='1.0',
#     packages=find_packages(), #include/exclude arguments take * as wildcard, . for any sub-package names
# )
import setuptools
setuptools.setup(
    name="DataFlexorMWAA",
    version="1.0.0",
    author="Mrinal Paul",
    author_email="",
    description="",
    long_description='',
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[ "Programming Language :: Python :: 3", "", "", ],
    python_requires='>=3.10',
)