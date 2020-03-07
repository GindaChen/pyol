import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyopenlambda",
    version="0.0.1",
    author="gindachen",
    author_email="homtazhan@gmail.com",
    description="A Python interface for OpenLambda",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gindachen/pyol",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)
