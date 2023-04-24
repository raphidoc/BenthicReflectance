import os
import msi_acolite_rhow as msi

UserHome = os.path.expanduser("~")

test = msi.msi_acolite(
    BBox = [-67.71270792889858, 49.28940909966143, -67.67082439228373, 49.30749494507612],
    Date = ('20190704', '20190705'),
    CloudCoverPercentage = (0, 10),
    TmpPath = os.path.join(UserHome, "tmp_rw")
)

test.get_rhow()