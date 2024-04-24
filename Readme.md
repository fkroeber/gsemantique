# global semantic EO data cubes 

[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

## Package description
<img src="docs/py_logo.png" align="right" width="150" />

This package builds on top of [semantique](https://zgis.github.io/semantique/#) and extends its functionality by offering:
1. Pre-configured access to a variety of EO datasets with global coverage (e.g. Sentinel-1, Sentinel-2, Landsat)
2. Retrieval & storage mechanisms for the data to persist data cube inputs locally and simplify the rebuilding of the data cubes and replication of the analyses
3. Scaling mechanisms that allow to evaluate recipes for large spatio-temporal extents up to the mesoscale, with internal automatic handling of the required chunking of the processing into smaller parts

## Installation

At this moment the package can only be installed from source. This can be done in several ways:

1) Using pip to install directly from GitHub:

```
pip install git+https://github.com/fkroeber/gsemantique.git
```

2) Cloning the repository first and then install with pip:

```
git clone https://github.com/fkroeber/gsemantique.git
cd gsemantique
pip install .     # install in non-editable mode
#pip install -e . # install in editable mode 
```