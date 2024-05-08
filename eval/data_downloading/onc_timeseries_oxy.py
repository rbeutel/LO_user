#########################################################
# Matching model output to mooring data from ONC ERDAPP #
#########################################################

# run:
# python3 current_match.py -id

import argparse
from erddapy import ERDDAP
import xarray as xr
import pandas as pd
import numpy as np

from datetime import timedelta
from pathlib import Path
from lo_tools import zfun, zrfun

#allow station id to be input in the command line
parser = argparse.ArgumentParser(description='Get and match ERDAPP data from ONC.')
parser.add_argument('id', type=str,
                    help='dataset/timeseries id')
args = parser.parse_args()

id = args.id


# function to get the model file path:
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


# download obs from ERDDAP
e = ERDDAP(
  server="http://dap.onc.uvic.ca/erddap",
  protocol="tabledap",
)

e.response = "nc"
e.dataset_id = id
e.constraints = None 

e.variables = [  
    "time",
    "oxygen_corrected",
    "longitude",
    "latitude",
    "depth"
]

df = e.to_pandas()
df['time (UTC)'] = pd.to_datetime(df['time (UTC)'])
print('erddap worked')

# convert to hourly - some of the mooring data is recorded every minute! wild!
df.set_index('time (UTC)',inplace=True)
df = df.resample('h',axis=0).mean()
df['datetime'] = np.array(df.index)
index = pd.Index(range(len(df)))
df.set_index(index,inplace=True)

# make some empty columns to fill the model data into:
df['model_o'] = np.nan

for cid in df.index:
    print(cid)
    lon = df.loc[cid,'longitude (degrees_east)']
    lat = df.loc[cid,'latitude (degrees_north)']
    depth = df['depth (m)'][cid]
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

        with xr.open_dataset(fn) as f:
            # convert to observed units
            df.loc[cid,'model_o'] = f.oxygen[0,iz,iy,ix].values  / 44.661 # convert mmol/m3 to ml/l
        # print(df.loc[cid,'model_o'])
    
    else:
        pass

# and pickle it
name = '/data1/bbeutel/LO_output/extract_cast/onc/' + id + '_oxy.p'
print(name)
df.to_pickle(name)