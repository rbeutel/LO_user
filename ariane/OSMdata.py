# lets get the OSM output into a nice formate for visualisations
import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# summer runs TS
s17 = xr.open_dataset('./OSM/20171020/ariane_positions_quantitative.nc')
s18 = xr.open_dataset('./OSM/20180920/ariane_positions_quantitative.nc')
s19 = xr.open_dataset('./OSM/20191104/ariane_positions_quantitative.nc')
s20 = xr.open_dataset('./OSM/20201019/ariane_positions_quantitative.nc')
s21 = xr.open_dataset('./OSM/20211005/ariane_positions_quantitative.nc')

# winter runs TS
w17 = xr.open_dataset('./OSM/20180224/ariane_positions_quantitative.nc')
w18 = xr.open_dataset('./OSM/20190416/ariane_positions_quantitative.nc')
w19 = xr.open_dataset('./OSM/20200219/ariane_positions_quantitative.nc')
w20 = xr.open_dataset('./OSM/20210219/ariane_positions_quantitative.nc')

#############
# Transport #
#############

# first average transport from each boundary over the whole season

# def get_stats(data, section, southdiv):
#     #function to get the average transport at each section over a given season
#     #south div is to set up boolean for the three south water masses, 1=CUC, 2=south shelf/davidson, 3=Columbia, 0=NA 

#     if section == 0:
#         boolean = (data.final_section == section) & ((data.init_t-data.final_t)>24) # to remove tidally pumped parcels across PRT
#     elif section == 2:
#         high = 33.9 # salinity division between CUC and south shelf
#         low = 32 # salinity division between south shelf and Columbia
#         if southdiv == 1: # CUC
#             boolean = (data.final_section == section) & (data.final_salt >= high)
#         elif southdiv == 2:
#             boolean = (data.final_section == section) & (data.final_salt < high) & (data.final_salt > low)
#         elif southdiv == 3:
#             boolean = (data.final_section == section) & (data.final_salt <= low)
#         else:
#             print('the options for the southern boundary are 1=CUC, 2=south shelf/davidson, 3=Columbia')
#     else:
#         boolean = (data.final_section == section)

#     tran = np.sum(data.init_transp[boolean])/(np.max(data.init_t)-1440)

#     return float(tran.values)

# df = {'section':['North','Offshore','CUC','South','Columbia','loop'],
#      's17':[get_stats(s17,4,0),get_stats(s17,3,0),get_stats(s17,2,1),get_stats(s17,2,2),get_stats(s17,2,3),get_stats(s17,0,0)],
#      's18':[get_stats(s18,4,0),get_stats(s18,3,0),get_stats(s18,2,1),get_stats(s18,2,2),get_stats(s18,2,3),get_stats(s18,0,0)],
#      's19':[get_stats(s19,4,0),get_stats(s19,3,0),get_stats(s19,2,1),get_stats(s19,2,2),get_stats(s19,2,3),get_stats(s19,0,0)],
#      's20':[get_stats(s20,4,0),get_stats(s20,3,0),get_stats(s20,2,1),get_stats(s20,2,2),get_stats(s20,2,3),get_stats(s20,0,0)],
#      's21':[get_stats(s21,4,0),get_stats(s21,3,0),get_stats(s21,2,1),get_stats(s21,2,2),get_stats(s21,2,3),get_stats(s21,0,0)],
#      'w17':[get_stats(w17,4,0),get_stats(w17,3,0),get_stats(w17,2,1),get_stats(w17,2,2),get_stats(w17,2,3),get_stats(w17,0,0)],
#      'w18':[get_stats(w18,4,0),get_stats(w18,3,0),get_stats(w18,2,1),get_stats(w18,2,2),get_stats(w18,2,3),get_stats(w18,0,0)],
#      'w19':[get_stats(w19,4,0),get_stats(w19,3,0),get_stats(w19,2,1),get_stats(w19,2,2),get_stats(w19,2,3),get_stats(w19,0,0)],
#      'w20':[get_stats(w20,4,0),get_stats(w20,3,0),get_stats(w20,2,1),get_stats(w20,2,2),get_stats(w20,2,3),get_stats(w20,0,0)]}

# print('dictionary made.\n')
# trans = pd.DataFrame(df)
# trans.to_csv('OSM/seasonal_transport.csv')
# print('transport done.\n')

###########
# Tracers #
###########
# the goal is to make a box plot for each tracer, with a different box for each section

salt = np.array([])
temp = np.array([])
season = np.array([])
year = np.array([])
section = np.array([])
trans = np.array([])
depth = np.array([])

def get_tracers(data,season,year):
    #function to get the temperature, and salinity + section info for each parcel
    #south div is to set up boolean for the three south water masses, 1=CUC, 2=south shelf/davidson, 3=Columbia, 0=NA 

    _salt = data.final_salt
    _temp = data.final_temp
    _depth = data.final_depth
    _trans = data.final_temp/(np.max(data.init_t)-1440)
    
    _season = [season]*len(_salt)
    _year = [year]*len(_salt)
    _section = np.zeros(len(data.final_section))
    
    _section =np.where(((data.final_section == 0) & ((data.init_t-data.final_t)>24)),'loop',_section)
    _section =np.where(((data.final_section == 3)),'offshore',_section)
    _section =np.where(((data.final_section == 4)),'north',_section)

    high = 33.9 # salinity division between CUC and south shelf
    low = 32 # salinity division between south shelf and Columbia
    _section =np.where((data.final_section == 2) & (data.final_salt >= high),'cuc',_section)
    _section =np.where((data.final_section == 2) & (data.final_salt < high) & (data.final_salt > low),'south',_section)
    _section =np.where((data.final_section == 2) & (data.final_salt <= low),'columbia',_section)

    return _salt, _temp, _trans, _depth, _season, _year, _section

# summer 2017
_salt, _temp, _transport, _depth, _season, _year, _section= get_tracers(s17, 'summer', 2017)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('s17.\n')

# winter 2017
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(w17, 'winter', 2017)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('w17.\n')

# summer 2018
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(s18, 'summer', 2018)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('s18.\n')

# winter 2018
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(w18, 'winter', 2018)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('w18.\n')

# summer 2019
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(s19, 'summer', 2019)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('s19.\n')

# winter 2019
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(w19, 'winter', 2019)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('w19.\n')

# summer 2020
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(s20, 'summer', 2020)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('s20.\n')

# winter 2020
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(w20, 'winter', 2020)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('w20.\n')

# summer 2021
_salt, _temp, _transport, _depth, _season, _year, _section = get_tracers(s21, 'summer', 2021)
salt = np.append(salt,_salt)
temp = np.append(temp,_temp)
trans = np.append(trans,_transport)
depth = np.append(depth,_depth)
season = np.append(season,_season)
year = np.append(year,_year)
section = np.append(section,_section)
print('s21.\n')

df = {'salt':salt, 'temperature':temp, 'transport':trans, 'depth':depth, 'season': season, 'year':year, 'section':section}
tracers = pd.DataFrame(df)
tracers = tracers.drop(tracers[tracers.section == '0.0'].index) # drop the tidally pumped data
tracers.to_csv('OSM/tracers.csv')