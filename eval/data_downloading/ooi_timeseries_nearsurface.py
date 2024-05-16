#####################################################
# Matching model output to mooring data from NANOOS #
#####################################################

# run:
# python3 ooi_timeseries_nearsurface.py -id -num1 -num2 (ex. ooi-ce06issm 16 16)
# make sure to change num1 and num2

import argparse
from erddapy import ERDDAP
import xarray as xr
import pandas as pd
import numpy as np
import gsw

from datetime import timedelta
from pathlib import Path
from lo_tools import zfun, zrfun

#allow station id to be input in the command line
parser = argparse.ArgumentParser(description='Get and match ERDAPP data from ONC.')
parser.add_argument('id', type=str,
                    help='asset id (lowercase)')
parser.add_argument('num1', type=str,
                    help='num1 (interger string)')
parser.add_argument('num2', type=str,
                    help='num2 (interger string)')
args = parser.parse_args()

id = args.id
num1 = args.num1
num2 = args.num2

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
# more confusing for OOI bc the data is fit into different profile names despite being from the same mooring
# so need to download from erdapp many times and append together

# num1 = str(27)
# num2 = str(26)
#         nearsurface CTD,           nearsurface velocity,      near surface oxygen,        near surface nitrate
suffix = ['-rid'+num1+'-03-ctdbpc000','-rid'+num2+'-04-velpta000','-rid'+num1+'-04-dostad000', '-rid'+num2+'-07-nutnrb000']


# CTD first
e = ERDDAP(
  server="http://erddap.dataexplorer.oceanobservatories.org/erddap",
  protocol="tabledap",
)
e.response = "nc"
e.dataset_id = id + suffix[0]
e.constraints = {'time>=': '2012-12-31T00:00:00Z', 'time<=': '2024-01-01T00:00:00Z'}
e.variables = [  
    "time",
    "longitude",
    "latitude",
    "sea_water_pressure",
    "sea_water_density",
    "sea_water_temperature",
    "sea_water_practical_salinity"
]
df = e.to_pandas()
df['time (UTC)'] = pd.to_datetime(df['time (UTC)'])
# convert to hourly - some of the mooring data is recorded every minute! wild!
df.set_index('time (UTC)',inplace=True)
df = df.resample('h',axis=0).mean()
# print('CTD length= '+str(len(df))+', date range= '+str(np.min(df.datetime))+'-'+str(np.max(df.datetime)))
d_ctd = df


# then velocity
e = ERDDAP(
  server="http://erddap.dataexplorer.oceanobservatories.org/erddap",
  protocol="tabledap",
)
e.response = "nc"
e.dataset_id = id + suffix[1]
e.constraints = {'time>=': '2012-12-31T00:00:00Z', 'time<=': '2024-01-01T00:00:00Z'}
e.variables = [  
    "time",
    "eastward_sea_water_velocity",
    "northward_sea_water_velocity",
    "upward_sea_water_velocity"
]
df = e.to_pandas()
df['time (UTC)'] = pd.to_datetime(df['time (UTC)'])
# convert to hourly - some of the mooring data is recorded every minute! wild!
df.set_index('time (UTC)',inplace=True)
df = df.resample('h',axis=0).mean()
# print('Velocity length= '+str(len(df))+', date range= '+str(np.min(df.datetime))+'-'+str(np.max(df.datetime)))
d_v = df

# oxygen 
e = ERDDAP(
  server="http://erddap.dataexplorer.oceanobservatories.org/erddap",
  protocol="tabledap",
)
e.response = "nc"
e.dataset_id = id + suffix[2]
e.constraints = {'time>=': '2012-12-31T00:00:00Z', 'time<=': '2024-01-01T00:00:00Z'}
e.variables = [  
    "time",
    "mole_concentration_of_dissolved_molecular_oxygen_in_sea_water"
]
df = e.to_pandas()
df['time (UTC)'] = pd.to_datetime(df['time (UTC)'])
# convert to hourly - some of the mooring data is recorded every minute! wild!
df.set_index('time (UTC)',inplace=True)
df = df.resample('h',axis=0).mean()
# print('Oxygen length= '+str(len(df))+', date range= '+str(np.min(df.datetime))+'-'+str(np.max(df.datetime)))
d_o = df


# nitrate 
e = ERDDAP(
  server="http://erddap.dataexplorer.oceanobservatories.org/erddap",
  protocol="tabledap",
)
e.response = "nc"
e.dataset_id = id + suffix[3]
e.constraints = {'time>=': '2012-12-31T00:00:00Z', 'time<=': '2024-01-01T00:00:00Z'}
e.variables = [  
    "time",
    "mole_concentration_of_nitrate_in_sea_water",
    "mole_concentration_of_nitrate_in_sea_water_full"
]
df = e.to_pandas()
df['time (UTC)'] = pd.to_datetime(df['time (UTC)'])
# convert to hourly - some of the mooring data is recorded every minute! wild!
df.set_index('time (UTC)',inplace=True)
df = df.resample('h',axis=0).mean()
# print('Nitrate length= '+str(len(df))+', date range= '+str(np.min(df.datetime))+'-'+str(np.max(df.datetime)))
d_n = df

df = pd.concat([d_ctd, d_v, d_o, d_n], axis=1)
df['datetime'] = np.array(df.index)
index = pd.Index(range(len(df)))
df.set_index(index,inplace=True)
print(len(df))


# make some empty columns to fill the model data into:
df['model_s'] = np.nan
df['model_t'] = np.nan
df['model_o'] = np.nan
df['model_n'] = np.nan
df['model_u'] = np.nan
df['model_v'] = np.nan
df['model_w'] = np.nan

# and fill away!
for cid in df.index:
    print(cid)
    lon = df.loc[cid,'longitude (degrees_east)']
    lat = df.loc[cid,'latitude (degrees_north)']
    depth = gsw.z_from_p(df['sea_water_pressure (decibars)'][cid],lat,lon)
    dt = df['datetime'][cid]

    fn = get_his_fn_from_dt(dt)

    if fn.is_file():

        G, S, T = zrfun.get_basic_info(fn)
        Lon = G['lon_rho'][0,:]
        Lat = G['lat_rho'][:,0]
        z_rho = zrfun.get_z(G['h'],np.zeros(np.shape(G['h'])),S,only_rho=True)

        ix = zfun.find_nearest_ind(Lon, lon)
        iy = zfun.find_nearest_ind(Lat, lat)
        iz = zfun.find_nearest_ind(z_rho[:,iy,ix],depth)

        with xr.open_dataset(fn, engine="h5netcdf") as f:
            s = f.salt[0,iz,iy,ix].values
            p = gsw.p_from_z(depth, lat)
            #convert to observation units:
            df.loc[cid,'model_s'] = gsw.SP_from_SA(s,p,lon,lat) # practical salinity
            df.loc[cid,'model_t'] = f.temp[0,iz,iy,ix].values # celcius already
            df.loc[cid,'model_o'] = f.oxygen[0,iz,iy,ix].values
            df.loc[cid,'model_n'] = f.NO3[0,iz,iy,ix].values
            df.loc[cid,'model_u'] = f.u[0,iz,iy,ix].values 
            df.loc[cid,'model_v'] = f.v[0,iz,iy,ix].values
            df.loc[cid,'model_w'] = f.w[0,iz,iy,ix].values
            f.close()

    else:
        pass

# and pickle it
name = '/data1/bbeutel/LO_output/extract_cast/ooi/' + id + '_ns.p'
print(name)
df.to_pickle(name)