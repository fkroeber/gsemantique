from setuptools import setup, find_packages

# Get the long description from the README file.
with open("README.md", encoding="utf-8") as file:
    long_description = file.read()

# List dependencies.
dependencies = [
    "aiohttp-retry",
    "multiprocess",
    "planetary-computer @ git+https://github.com/fkroeber/planetary-computer-sdk-for-python.git",
    "semantique @ git+https://github.com/fkroeber/semantique.git@merged_II",
    "stac-asset",
    "stackstac @ git+https://github.com/fkroeber/stackstac.git",
    "tqdm",
]

# List development dependencies.
dev_dependencies = [
    "matplotlib",
    "pytest",
    "flake8",
    "sphinx",
    "mypy",
    "black",
]


# Setup.
setup(
    name="gsemantique",
    version="0.1.0",
    description="Global semantic EO dataset querying",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/fkroeber/gsemantique",
    author="Felix Kröber",
    author_email="felix.kroeber@plus.ac.at",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=dependencies,
    extras_require={"dev": dev_dependencies},
)
