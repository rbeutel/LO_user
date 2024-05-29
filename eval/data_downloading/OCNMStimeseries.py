# matching OCNMS mooring data to model
# looking at 2014 and 2015

# NOTE - patchy data and velocity units dont seem to be m/s

import scipy.io
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pathlib import Path
from lo_tools import zfun, zrfun
import xarray as xr

# functions
def datenum_to_datetime(datenum):
    unix_epoch = datetime(1970, 1, 1)
    matlab_epoch_difference = 719529  # Number of days between 0000-01-01 and 1970-01-01
    return unix_epoch + timedelta(days=datenum - matlab_epoch_difference)

def get_his_fn_from_dt(dt):

    path = Path("/agdat1/parker/LO_roms")
    # This creates the Path of a history file from its datetime
    if dt.hour == 0:
        # perfect restart does not write the 0001 file
        dt = dt - timedelta(days=1)
        his_num = '0025'
    else:
        his_num = ('0000' + str(dt.hour + 1))[-4:]
    date_string = dt.strftime('%Y.%m.%d')
    fn = path / 'cas7_t0_x4b' / ('f' + date_string) / ('ocean_his_' + his_num + '.nc')
    return fn

################
# OBSERVATIONS #
################
# upload data
data14 = scipy.io.loadmat('/data1/bbeutel/LO_user/eval/data_downloading/2014.mat')
data15 = scipy.io.loadmat('/data1/bbeutel/LO_user/eval/data_downloading/2015.mat')

# setup arrays you're gonna fill
time = np.array([])
VE = np.array([])
VN = np.array([])

# plus the stuff that constant
# from https://olympiccoast.noaa.gov/science/oceanographic-moorings/#:~:text=The%20moorings%20have%20a%20lightweight,deployed%20in%20the%20same%20location.
lon = -124.57333 #124º 29.324'
lat = 47.402222 #47º 21.188'		
depth = 42

### 2014 ###
keys = [k for k in data14.keys() if k[:5]=='VT_CE']

for k in keys:
    vt_data = data14[k]
    data_info = vt_data['data'][0,0]

    # extract the data
    VE = np.append(VE, data_info['AVE'][0][0])
    VN = np.append(VN, data_info['AVN'][0][0])
    time = np.append(time, data_info['time'][0][0])

### 2015 ###
keys = [k for k in data15.keys() if k[:5]=='VT_CE']

for k in keys:
    vt_data = data15[k]
    data_info = vt_data['data'][0,0]

    # extract the data
    VE = np.append(VE, data_info['AVE'][0][0])
    VN = np.append(VN, data_info['AVN'][0][0])
    time = np.append(time, data_info['time'][0][0])

# convert to actual dates
date_time = [datenum_to_datetime(t) for t in time]

# Create a DataFrame
d = {
    'datetime': date_time,
    'obs_u': VE,
    'obs_v': VN
}
df = pd.DataFrame(d)

#########
# MODEL #
#########

# make some empty columns to fill the model data into:
df['model_u'] = np.nan
df['model_v'] = np.nan

# and fill away!
for cid in df.index:
    print(cid)
    dt = df['datetime'][cid]

    fn = get_his_fn_from_dt(dt)

    if fn.is_file():

        G, S, T = zrfun.get_basic_info(fn)
        Lon = G['lon_rho'][0,:]
        Lat = G['lat_rho'][:,0]
        z_rho = zrfun.get_z(G['h'],np.zeros(np.shape(G['h'])),S,only_rho=True)

        ix = zfun.find_nearest_ind(Lon, lon)
        iy = zfun.find_nearest_ind(Lat, lat)
        iz = zfun.find_nearest_ind(z_rho[:,iy,ix],depth*-1)

        with xr.open_dataset(fn, engine="h5netcdf") as f:
            df.loc[cid,'model_u'] = f.u[0,iz,iy,ix].values 
            df.loc[cid,'model_v'] = f.v[0,iz,iy,ix].values
    
    else:
        pass

# and pickle it
name = '/data1/bbeutel/LO_output/extract_cast/ocnms_CE042_uv.p'
print(name)
df.to_pickle(name)