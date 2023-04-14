import sys, os, time

UserHome = os.path.expanduser("~")
sys.path.append(os.path.join(UserHome, "acolite"))
import acolite as ac

import geojson
import geopandas as gpd
import json
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from shapely.geometry import Polygon, box
import shutil
import xarray as xr

# Need the geocube.api.core to make ACOLITE work ? PROJ error otherwise ... https://odnature.naturalsciences.be/remsem/acolite-forum/viewtopic.php?t=319
# Something usefull to ACOLITE may get mask in the import ...
from geocube.api.core import make_geocube

#  Can input username and password here in place of None or put them in an .netrc file in your user home directory in the following form:
# machine apihub.copernicus.eu
# login <your username>
# password <your password>
sentinelapi = SentinelAPI(None, None, 'https://apihub.copernicus.eu/apihub')

#bbox = [lonmin, latmin, lonmax, latmax]
bbox = [-67.71270792889858, 49.28940909966143, -67.67082439228373, 49.30749494507612]
date = ('20190704', '20190705')
cloudcoverpercentage = (0, 10)

def water_reflectance_msi(sentinelapi, bbox, date, cloudcoverpercentage):

    # create a tmp folder to work in
    TmpPath = os.path.join(UserHome, "tmp_rw")
    try:
        os.makedirs(TmpPath)
    except FileExistsError:
        print("FileExistsError")
        pass

    # create a bounding box in WKT format
    bbox = box(*bbox)
    bbox_json = json.loads(gpd.GeoSeries([bbox]).to_json())
    bbox_wkt = geojson_to_wkt(bbox_json)

    Products = sentinelapi.query(bbox_wkt,
                         date=date,
                         platformname='Sentinel-2',
                         producttype='S2MSI1C',
                         cloudcoverpercentage=cloudcoverpercentage)

    # Create directory to store the L1C products
    L1cPath = os.path.join(TmpPath, "L1C")
    try:
        os.makedirs(L1cPath)
    except FileExistsError:
        print("FileExistsError")
        pass

    # Get the UUID of the product to download
    GeoData = sentinelapi.to_geodataframe(Products)
    UUID = GeoData['uuid'][0]

    DownloadResult = sentinelapi.download_all(
        products = [UUID],
        directory_path = L1cPath,
        max_attempts = 10,
        checksum = True,
        n_concurrent_dl = sentinelapi.concurrent_dl_limit,
        lta_retry_delay = 1800,
        fail_fast = False,
        nodefilter = None
    )

    # Create directory for ACOLITE processing
    AcolitePath = os.path.join(TmpPath, "ac")
    try:
        os.makedirs(AcolitePath)
    except FileExistsError:
        print("FileExistsError")
        pass

    # Unzip the L1C product to input the SAFE folder to acolite
    shutil.unpack_archive(DownloadResult[0][UUID]["path"], AcolitePath)
    InFile = os.path.join(AcolitePath,DownloadResult[0][UUID]["title"]+".SAFE")

    # Save the bounding box as a geojson file to use as a polygon limit in ACOLITE processing
    TmpGeojson = os.path.join(AcolitePath,"polygon_limit.geojson")
    with open(TmpGeojson, 'w') as outfile:
        geojson.dump(bbox_json, outfile)

    # Define the ACOlITE settings for the atmospheric compensation processing
    acolitesettings = {"inputfile":InFile,
         "output":AcolitePath,
         "polygon":TmpGeojson,
         "polygon_limit":True,
         "l2w_parameters":['rhow_*'],
         "s2_target_res":10,
         #"output_xy":True,
         #"reproject_before_ac":True,
         #"output_projection_epsg":2960,
         "dsf_residual_glint_correction":True
         }

    AcoliteResult = ac.acolite.acolite_run(settings=acolitesettings)

    L2Array = xr.open_dataset(AcoliteResult[0]['l2w'][0])
    df = L2Array.to_dataframe()
    df = df.reset_index()
    df = df.drop(columns=['x', 'y', 'transverse_mercator', 'l2_flags'])
    df = df.rename(columns={'lon': 'x', 'lat': 'y'})
    df.to_csv(os.path.join(AcolitePath,DownloadResult[0][UUID]["title"]+"L2W.xy"), sep=',', header=True, index=False)
