import os
import msi_acolite_rhow as msi
import pandas as pd

UserHome = os.path.expanduser("~")

Surface = pd.read_csv(
    "/data/Benthic/Surfaces/20191012-13_Godbout_Godbout_NAD83(SCRS)_UTM19N_ZC_MareeGPS_PPK_CUBE_1m.txt",
    sep=" ", header=None, names=['x', 'y', 'z', 'StdDev', 'Density'])[["x", "y", "z"]]
Surface['z'] = -Surface.z

test = msi.msi_acolite(
    Surface = Surface,
    TideHeight=0,
    EPSG = '2960',
    Date = ('20190704', '20190705'),
    CloudCoverPercentage = (0, 10),
    TmpPath = os.path.join(UserHome, 'tmp_rw')
)

test.get_rhob()