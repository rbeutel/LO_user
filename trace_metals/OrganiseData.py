import xarray as xr
import pandas as pd
import numpy as np
import datetime as dt

df = xr.open_dataset("/data1/bbeutel/LO_user/ariane/metals/20170101/ariane_positions_quantitative.nc")

bdy_loo = 0
bdy_sou = 4
bdy_off = 2
bdy_nor = 3

bool_sout = (df.final_section==bdy_sou)
bool_nort = (df.final_section==bdy_nor)
bool_loop = (df.final_section==bdy_loo)& (abs(df.init_t-df.final_t) > 24)
bool_offs = (df.final_section==bdy_off)


# Making a CSV of daily transport #
length =int(np.max(df.init_t)/24)
startday = dt.datetime(2017, 1, 1)
dates = [startday+dt.timedelta(days=i) for i in range(length)] 

# d = {}
# d['dates'] = dates
# d['south'] = [np.sum(df.final_transp[bool_sout & (df.init_t >= 1+(i*24)) & (df.init_t <= 1+(i*24)+23)]).values/24 for i in range(length)]
# d['north'] = [np.sum(df.final_transp[bool_nort & (df.init_t >= 1+(i*24)) & (df.init_t <= 1+(i*24)+23)]).values/24 for i in range(length)]
# d['loop'] = [np.sum(df.final_transp[bool_loop & (df.init_t >= 1+(i*24)) & (df.init_t <= 1+(i*24)+23)]).values/24 for i in range(length)]
# d['offshore'] = [np.sum(df.final_transp[bool_offs & (df.init_t >= 1+(i*24)) & (df.init_t <= 1+(i*24)+23)]).values/24 for i in range(length)]
 
# day_transp = pd.DataFrame(d)
# day_transp.to_csv('/data1/bbeutel/LO_user/trace_metals/daily_transport.csv')
# print('/data1/bbeutel/LO_user/trace_metals/daily_transport.csv')

# then making separate csv files for ALL parcels in each of the water masses

d = {}
d['transport'] = df.init_transp[bool_sout]
d['init_lon'] = df.init_lon[bool_sout]
d['init_lat'] = df.init_lat[bool_sout]
d['init_depth'] = df.init_depth[bool_sout]
d['init_time'] = df.init_t[bool_sout]
d['init_temp'] = df.init_temp[bool_sout]
d['init_salt'] = df.init_salt[bool_sout]
d['final_lon'] = df.init_lon[bool_sout]
d['final_lat'] = df.init_lat[bool_sout]
d['final_depth'] = df.final_depth[bool_sout]
d['final_time'] = df.final_t[bool_sout]
d['final_temp'] = df.final_temp[bool_sout]
d['final_salt'] = df.final_salt[bool_sout]
south = pd.DataFrame(d)
south.to_csv('/data1/bbeutel/LO_user/trace_metals/south.csv')
print('/data1/bbeutel/LO_user/trace_metals/south.csv')

d = {}
d['transport'] = df.init_transp[bool_nort]
d['init_lon'] = df.init_lon[bool_nort]
d['init_lat'] = df.init_lat[bool_nort]
d['init_depth'] = df.init_depth[bool_nort]
d['init_time'] = df.init_t[bool_nort]
d['init_temp'] = df.init_temp[bool_nort]
d['init_salt'] = df.init_salt[bool_nort]
d['final_lon'] = df.init_lon[bool_nort]
d['final_lat'] = df.init_lat[bool_nort]
d['final_depth'] = df.final_depth[bool_nort]
d['final_time'] = df.final_t[bool_nort]
d['final_temp'] = df.final_temp[bool_nort]
d['final_salt'] = df.final_salt[bool_nort]
north = pd.DataFrame(d)
north.to_csv('/data1/bbeutel/LO_user/trace_metals/north.csv')
print('/data1/bbeutel/LO_user/trace_metals/north.csv')

d = {}
d['transport'] = df.init_transp[bool_offs]
d['init_lon'] = df.init_lon[bool_offs]
d['init_lat'] = df.init_lat[bool_offs]
d['init_depth'] = df.init_depth[bool_offs]
d['init_time'] = df.init_t[bool_offs]
d['init_temp'] = df.init_temp[bool_offs]
d['init_salt'] = df.init_salt[bool_offs]
d['final_lon'] = df.init_lon[bool_offs]
d['final_lat'] = df.init_lat[bool_offs]
d['final_depth'] = df.final_depth[bool_offs]
d['final_time'] = df.final_t[bool_offs]
d['final_temp'] = df.final_temp[bool_offs]
d['final_salt'] = df.final_salt[bool_offs]
offshore = pd.DataFrame(d)
offshore.to_csv('/data1/bbeutel/LO_user/trace_metals/offshore.csv')
print('/data1/bbeutel/LO_user/trace_metals/offshore.csv')

d = {}
d['transport'] = df.init_transp[bool_loop]
d['init_lon'] = df.init_lon[bool_loop]
d['init_lat'] = df.init_lat[bool_loop]
d['init_depth'] = df.init_depth[bool_loop]
d['init_time'] = df.init_t[bool_loop]
d['init_temp'] = df.init_temp[bool_loop]
d['init_salt'] = df.init_salt[bool_loop]
d['final_lon'] = df.init_lon[bool_loop]
d['final_lat'] = df.init_lat[bool_loop]
d['final_depth'] = df.final_depth[bool_loop]
d['final_time'] = df.final_t[bool_loop]
d['final_temp'] = df.final_temp[bool_loop]
d['final_salt'] = df.final_salt[bool_loop]
loop = pd.DataFrame(d)
loop.to_csv('/data1/bbeutel/LO_user/trace_metals/loop.csv')
print('/data1/bbeutel/LO_user/trace_metals/loop.csv')