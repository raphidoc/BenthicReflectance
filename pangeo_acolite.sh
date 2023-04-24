#!/bin/bash
# This script is used to install the dependencies for the benthic reflectance project

cd ~

# Install mamaforge, the fast c++ implementation of conda
#wget https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh
#./Mambaforge-Linux-x86_64.sh
#bash
#conda config --set auto_activate_base false

# Create a new environment called pangeo-acolite and install the pangeo packages
mamba create -n pangeo-acolite -c conda-forge \
    python dask jupyterlab dask-jobqueue ipywidgets \
    xarray zarr numcodecs hvplot geoviews datashader  \
    jupyter-server-proxy widgetsnbextension dask-labextension

# Activate the environment
mamba activate pangeo-acolite

# install the acolite dependencies
mamba install -c conda-forge numpy matplotlib scipy gdal pyproj scikit-image pyhdf \
    pyresample netcdf4 h5py requests pygrib cartopy

# Install acolite
git clone --depth 1 https://github.com/acolite/acolite

# Install specific dependencies for the project
mamba install -c conda-forge geojson geopandas geocube sentinelsat

