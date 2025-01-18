# lets get the ariane output into a nice formate for easy sensitivity analysis of source water definitions
import datetime as dt
import xarray as xr
import pandas as pd
import numpy as np

# upwelling runs
upendday = [dt.datetime(2013, 10, 24), dt.datetime(2014, 9, 3), 
            dt.datetime(2015, 9, 5), dt.datetime(2016, 9, 13), 
            dt.datetime(2017, 10, 12), dt.datetime(2018, 9, 6), 
            dt.datetime(2020, 10, 17),
            dt.datetime(2021, 9, 22), dt.datetime(2022, 10, 15)]  # Start dates

# downwelling runs
dwendday = [dt.datetime(2014, 3, 6), dt.datetime(2016, 3, 19),dt.datetime(2017, 4, 19), dt.datetime(2018, 2, 1),
          dt.datetime(2020, 1, 27),dt.datetime(2021, 2, 2), dt.datetime(2022, 1, 25)]

###########
# Tracers #
###########
# make one big csv file with section (ariane output definition, not my definition), depth, year, transport salt, temp, DO, NO3, TA, DIC

def get_tracers(endday,updown):
    #function to get the temperature, and salinity + section info for each parcel
    #south div is to set up boolean for the three south water masses, 1=CUC, 2=south shelf/davidson, 3=Columbia, 0=NA 

    section = np.array([])
    depth = np.array([])
    lon = np.array([])
    lat = np.array([])
    year = np.array([])
    trans = np.array([])
    salt = np.array([])
    temp = np.array([])
    nit = np.array([])
    oxy = np.array([])
    ta = np.array([])
    dic = np.array([])

    st_files = ['/data1/bbeutel/LO_user/ariane/{}_cas7/S_T/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(updown,day) for day in endday]
    td_files = ['/data1/bbeutel/LO_user/ariane/{}_cas7/TA_DIC/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(updown,day) for day in endday]
    dn_files = ['/data1/bbeutel/LO_user/ariane/{}_cas7/DO_NO3/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(updown,day) for day in endday]

    for i in range(len(endday)):
        s_t = xr.open_dataset(st_files[i])
        do_no3 = xr.open_dataset(dn_files[i])
        ta_dic = xr.open_dataset(td_files[i])

        tides = ((abs(s_t.init_t-s_t.final_t) > 24) & ~np.isnan(s_t.final_section)) # boolean to ignore tidally pumped parcels and lost parcels

        section = np.append(section,s_t.final_section[tides])
        trans = np.append(trans,s_t.final_transp[tides])
        depth = np.append(depth,s_t.final_depth[tides])
        lon = np.append(lon,s_t.final_lon[tides])
        lat = np.append(lat,s_t.final_lat[tides])
        year = np.append(year,[endday[i].year] * len(s_t.final_section[tides]))
        salt = np.append(salt,s_t.final_salt[tides])
        temp = np.append(temp,s_t.final_temp[tides])
        nit = np.append(nit,do_no3.final_salt[tides])
        oxy = np.append(oxy,do_no3.final_temp[tides])
        ta = np.append(ta,ta_dic.final_temp[tides])
        dic = np.append(dic,ta_dic.final_salt[tides])


    d = {'year':year,  'section':section, 'depth':depth, 'lon':lon, 'lat':lat, 'transport':trans, 'salt':salt, 'temperature':temp, 'NO3':nit, 'DO': oxy, 'TA': ta, 'DIC':dic}
    df = pd.DataFrame(d)
    return df

up = get_tracers(upendday,'up')
up.to_csv('summary_files/upwellingdata.csv')
print('up done')

dw = get_tracers(dwendday,'down')
dw.to_csv('summary_files/downwellingdata.csv')
print('down done')