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

#bbox = [lonmin, latmin, lonmax, latmax]
#bbox = [-68.689360, 48.890805, -68.041661, 49.283823]
#BBox = [-67.71270792889858, 49.28940909966143, -67.67082439228373, 49.30749494507612]
#Date = ('20190704', '20190705')
#CloudCoverPercentage = (0, 10)
#TmpPath = os.path.join(UserHome, "tmp_rw")

#Class to download Sentinel-2 MSI data from Copernicus Hub and process it with ACOLITE to get the water reflectance values.

class msi_acolite:

    def __init__(self, BBox, Date, CloudCoverPercentage, TmpPath):

        #  Put username and passowrd in an .netrc file in your user home directory in the following form:
        # machine apihub.copernicus.eu
        # login <your username>
        # password <your password>
        self.SentinelAPI = SentinelAPI(None, None, 'https://apihub.copernicus.eu/apihub')

        # Transform BBox to JSON
        self.BBox = BBox
        self.BBox = box(*self.BBox)
        self.BBoxJSON = json.loads(gpd.GeoSeries([self.BBox]).to_json())
        
        self.Date = Date
        self.CloudCoverPercentage = CloudCoverPercentage
        self.TmpPath = TmpPath

    # Download the Sentinel-2 MSI data from Copernicus Hub
    def download_L1c(self):

        # create a tmp folder to work in
        #TmpPath = os.path.join(UserHome, "tmp_rw")
        try:
            os.makedirs(self.TmpPath)
        except FileExistsError:
            print("FileExistsError")
            pass

        # create a bounding box in WKT format
        BBoxWTK = geojson_to_wkt(self.BBoxJSON)

        Products = self.SentinelAPI.query(
            BBoxWTK,
            date=self.Date,
            platformname='Sentinel-2',
            producttype='S2MSI1C',
            cloudcoverpercentage=self.CloudCoverPercentage
        )

        # If no product found raise an error
        if len(Products) == 0:
            raise ValueError("No product found for the given parameters")

        # Create directory to store the L1C products
        L1cPath = os.path.join(self.TmpPath, "L1C")
        try:
            os.makedirs(L1cPath)
        except FileExistsError:
            print("FileExistsError")
            pass

        # Get the UUID of the product to download
        GeoData = self.SentinelAPI.to_geodataframe(Products)
        UUID = GeoData['uuid'][0]

        DownloadResult = self.SentinelAPI.download_all(
            products = [UUID],
            directory_path = L1cPath,
            max_attempts = 10,
            checksum = True,
            n_concurrent_dl = self.SentinelAPI.concurrent_dl_limit,
            lta_retry_delay = 1800,
            fail_fast = False,
            nodefilter = None
        )

        return DownloadResult, UUID

    def get_rhow(self):

        DownloadResult, UUID = self.download_L1c()

        # Create directory for ACOLITE processing
        AcolitePath = os.path.join(self.TmpPath, "ac")
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
            geojson.dump(self.BBoxJSON, outfile)

        # Define the ACOlITE settings for the atmospheric compensation processing
        acolitesettings = {"inputfile":InFile,
                           "output":AcolitePath,
                           "polygon":TmpGeojson,
                           "polygon_limit":True,
                           "l2w_parameters":['rhow_*'],
                           "s2_target_res":10,
                           #"output_xy":True,
                           # #"reproject_before_ac":True,
                           # #"output_projection_epsg":2960,
                           # "dsf_residual_glint_correction":True
                           }

        AcoliteResult = ac.acolite.acolite_run(settings=acolitesettings)

        L2Array = xr.open_dataset(AcoliteResult[0]['l2w'][0])
        df = L2Array.to_dataframe()
        df = df.reset_index()
        df = df.drop(columns=['x', 'y', 'transverse_mercator', 'l2_flags'])
        df = df.rename(columns={'lon': 'x', 'lat': 'y'})
        #df.to_csv(os.path.join(AcolitePath,DownloadResult[0][UUID]["title"]+"L2W.xy"), sep=',', header=True, index=False)

        # Write to x,y,rhow_*wavelength* to sys.stdout
        df.to_csv(sys.stdout, sep=',', header=True, index=False)

        # Return the dataframe with the water reflectance values, one line per pixel
        return df
