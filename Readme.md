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
pip install .
```

## Usage
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fkroeber/gsemantique/main)
[![Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](http://colab.research.google.com/github/fkroeber/gsemantique/blob/main)  

The package contains several [Jupyter demo notebooks](./demo/). They are providing an overview on the package's operating principles and some hands-on examples. If you want to avoid the installation setup effort and explore the notebooks in an interactive manner, you can make use of Binder or Google Colab. Simply click on the Binder or Google Colab badge. Doing so will setup an online environment for you with the package and all its dependencies installed. No installations or complicated system configurations required, you only need a web browser.

> Note that gsemantique is designed to enable mesoscale analysis of big EO data on common consumer hardware infrastructure. Still, there is a minimum of resources required when working with EO data at scale. Binder and Colab are offering very restricted resources [1, 2]. Therefore it is strongly recommended, to install the package locally and explore it this way.
> 
> [1] Binder sets up a JupyterLab env with 2 CPUs and a guaranteed RAM of at least 1GB (maximum of 4GB).  
> [2] For Colab, the default CPU is an Intel Xeon CPU with 2 vCPUs (virtual CPUs) with about 12GB of RAM. 


