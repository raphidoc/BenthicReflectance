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
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_points_griddata, rasterize_points_radial
from functools import partial
import numpy as np

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

    def __init__(self, Surface, TideHeight, EPSG, Date, CloudCoverPercentage, TmpPath):

        # Define the SentinelAPI for sentinelsat module (API search and download of satellite images from scihub.copernicus.eu)
        # Put username and password in an .netrc file in your user home directory in the following form:
        # machine apihub.copernicus.eu
        # login <your username>
        # password <your password>
        self.SentinelAPI = SentinelAPI(None, None, 'https://apihub.copernicus.eu/apihub')

        # EPSG numeric code of surface
        # EPSG = '2960'
        self.EPSG = EPSG

        # Convert Surface to Geodataframe
        self.Surface = Surface
        self.GeometrySurface = gpd.points_from_xy(self.Surface['x'], self.Surface['y'], crs="EPSG:"+str(self.EPSG))
        self.gdf = gpd.GeoDataFrame(self.Surface, geometry=self.GeometrySurface)

        # Create BBox from Surface
        self.BBox = box(*self.gdf.total_bounds)
        # Convert BBox to WGS84 for satellite image search only
        self.BBoxJSON = gpd.GeoSeries([self.BBox]).set_crs('EPSG:'+str(self.EPSG)).to_crs('EPSG:4326').to_json()
        self.BBoxJSON = json.loads(self.BBoxJSON)

        self.Date = Date
        self.CloudCoverPercentage = CloudCoverPercentage
        self.TmpPath = TmpPath

        # Optional TideHeight to use H (water column height) instead of Z (bottom altitude)
        self.TidelHeight = TideHeight

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
                           "l2w_parameters":['rhow_*', 'p3qaa_Kd_*'],
                           "s2_target_res":10,
                           "output_xy":True,
                           "reproject_before_ac":True,
                           "output_projection_epsg":self.EPSG,
                           "dsf_residual_glint_correction":True
                           }

        AcoliteResult = ac.acolite.acolite_run(settings=acolitesettings)

        L2Array = xr.open_dataset(AcoliteResult[0]['l2w'][0])
        df = L2Array.to_dataframe()
        df = df.reset_index()
        df = df.drop(columns=['x', 'y', 'transverse_mercator', 'l2_flags'])
        df = df.rename(columns={'lon': 'x', 'lat': 'y'})
        #df.to_csv(os.path.join(AcolitePath,DownloadResult[0][UUID]["title"]+"L2W.xy"), sep=',', header=True, index=False)

        # Write to x,y,rhow_*wavelength* to sys.stdout
        #df.to_csv(sys.stdout, sep=',', header=True, index=False)

        # Return the dataframe with the water reflectance values, one line per pixel
        return L2Array

    def get_rhob(self):

        RhoW = self.get_rhow()

        print('Rasterizing xyz surface')
        Hraster = make_geocube(
            self.gdf,
            measurements=["z"],
            resolution=(10, 10),
            rasterize_function=partial(rasterize_points_griddata, method="linear"),
        )
        print('Done')

        # Add the water level (H) to the bottom altitude (Z)
        # Should check the sign (Z as positive or negative ?)
        Hraster['z'] = Hraster['z']+self.TidelHeight

        Combined = xr.combine_by_coords([RhoW, Hraster], combine_attrs='override')

        BRI492 = Combined['rhow_492']/np.exp(-Combined['z']*Combined['p3qaa_Kd_492'])
        BRI559 = Combined['rhow_559']/np.exp(-Combined['z']*Combined['p3qaa_Kd_559'])
        BRI665 = Combined['rhow_665']/np.exp(-Combined['z']*Combined['p3qaa_Kd_665'])

        DF492 = (
            BRI492
            .to_dataframe(name='BRI_492')
            .reset_index()
        )[['y','x','BRI_492']]

        DF559 = (
            BRI559
            .to_dataframe(name='BRI_559')
            .reset_index()
        )[['y','x','BRI_559']]

        DF665 = (
            BRI665
            .to_dataframe(name='BRI_665')
            .reset_index()
        )[['y','x','BRI_665']]

        Merged = DF492.merge(DF559, on=('y','x'), how='left')
        Merged = Merged.merge(DF665, on=('y', 'x'), how='left')

        Merged.to_csv(sys.stdout, sep=',', header=True, index=False)
