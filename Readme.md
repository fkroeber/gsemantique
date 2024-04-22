# gsemantique - global semantic EO dataset querying ![alt text](docs/py_logo.png)

## Package description

This package builds on top of [semantique](https://zgis.github.io/semantique/#) and extends its functionality by offering:
* Pre-configured access to a variety of EO datasets with global coverage (e.g., Sentinel-1, Sentinel-2, Landsat)
* Retrieval & storage mechanisms for the data to persist data cube inputs locally and simplify the rebuilding of the data cubes and replication of the analyses
* Scaling mechanisms that allow to evaluate recipes for large spatio-temporal extents up to the mesoscale, with internal automatic handling of the required chunking of the processing into smaller parts