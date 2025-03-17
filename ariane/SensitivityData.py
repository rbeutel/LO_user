# lets get the ariane output into a nice formate for easy sensitivity analysis of source water definitions
import datetime as dt
import xarray as xr
import pandas as pd
import numpy as np

# upwelling runs
upendday = [dt.datetime(2014, 9, 3), 
            dt.datetime(2015, 9, 5), dt.datetime(2016, 9, 13), 
            dt.datetime(2017, 10, 12), dt.datetime(2018, 9, 6), 
            dt.datetime(2019, 11, 5),dt.datetime(2020, 10, 17),
            dt.datetime(2021, 9, 22), dt.datetime(2022, 10, 15), 
            dt.datetime(2023, 9, 22)]

# downwelling runs
dwendday = [dt.datetime(2014, 3, 6), dt.datetime(2015, 2, 12), dt.datetime(2016, 3, 19),dt.datetime(2017, 4, 19), dt.datetime(2018, 2, 1),
          dt.datetime(2019, 4, 6), dt.datetime(2020, 1, 27),dt.datetime(2021, 2, 2), dt.datetime(2022, 1, 25),dt.datetime(2023, 4, 19)]

# spring runs
spendday = [dt.datetime(2014, 3, 25), 
            dt.datetime(2015, 4, 13), dt.datetime(2016, 4, 7), 
            dt.datetime(2017, 5, 8), dt.datetime(2018, 4, 30), 
            dt.datetime(2019, 4, 25),dt.datetime(2020, 2, 15),
            dt.datetime(2021, 3, 18), dt.datetime(2022, 6, 11), 
            dt.datetime(2023, 5, 8)]

# fall runs
flendday = [dt.datetime(2014, 9, 21), 
            dt.datetime(2015, 10, 21), dt.datetime(2016, 10, 2), 
            dt.datetime(2017, 11, 1), dt.datetime(2018, 10, 24), 
            dt.datetime(2019, 12, 4),dt.datetime(2020, 11, 11),
            dt.datetime(2021, 10, 6), dt.datetime(2022, 10,24), 
            dt.datetime(2023, 10, 13)]

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

# up = get_tracers(upendday,'up')
# up.to_csv('summary_files/upwellingdata.csv')
# print('up done')

# dw = get_tracers(dwendday,'down')
# dw.to_csv('summary_files/downwellingdata.csv')
# print('down done')

##################################################################################
# Combine data for a whole year instead of separating into upwelling/downwelling #
##################################################################################

def get_tracers_all(dwend,spend,upend,flend):
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
    # oxy = np.array([])
    # ta = np.array([])
    # dic = np.array([]) # don't actually look at these in the sensitivity analysis, so don't take for now

    dw_st_files = ['/data1/bbeutel/LO_user/ariane/down_cas7/S_T/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in dwend]
    # dw_td_files = ['/data1/bbeutel/LO_user/ariane/down_cas7/TA_DIC/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in dwend]
    dw_dn_files = ['/data1/bbeutel/LO_user/ariane/down_cas7/DO_NO3/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in dwend]
    sp_st_files = ['/data1/bbeutel/LO_user/ariane/buffer_cas7/Spring/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in spend]
    # sp_td_files = ['/data1/bbeutel/LO_user/ariane/buffer_cas7/SpringTADIC/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in spend]
    sp_dn_files = ['/data1/bbeutel/LO_user/ariane/buffer_cas7/SpringDONO3/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in spend]
    up_st_files = ['/data1/bbeutel/LO_user/ariane/up_cas7/S_T/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in upend]
    # up_td_files = ['/data1/bbeutel/LO_user/ariane/up_cas7/TA_DIC/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in upend]
    up_dn_files = ['/data1/bbeutel/LO_user/ariane/up_cas7/DO_NO3/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in upend]
    fl_st_files = ['/data1/bbeutel/LO_user/ariane/buffer_cas7/Fall/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in flend]
    # fl_td_files = ['/data1/bbeutel/LO_user/ariane/buffer_cas7/FallTADIC/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in flend]
    fl_dn_files = ['/data1/bbeutel/LO_user/ariane/buffer_cas7/FallDONO3/{:%Y%m%d}/ariane_positions_quantitative.nc'.format(day) for day in flend]

    for i in range(len(upendday)):
        dws_t = xr.open_dataset(dw_st_files[i])
        dwdo_no3 = xr.open_dataset(dw_dn_files[i])
        sps_t = xr.open_dataset(sp_st_files[i])
        spdo_no3 = xr.open_dataset(sp_dn_files[i])
        ups_t = xr.open_dataset(up_st_files[i])
        updo_no3 = xr.open_dataset(up_dn_files[i])
        fls_t = xr.open_dataset(fl_st_files[i])
        fldo_no3 = xr.open_dataset(fl_dn_files[i])

        dwtides = ((abs(dws_t.init_t-dws_t.final_t) > 24)) # boolean to ignore tidally pumped parcels 
        sptides = ((abs(sps_t.init_t-sps_t.final_t) > 24))
        uptides = ((abs(ups_t.init_t-ups_t.final_t) > 24))
        fltides = ((abs(fls_t.init_t-fls_t.final_t) > 24))
        # not removing lost parcels for now bc we want to quantify these

        section = np.append(section,np.append(dws_t.final_section[dwtides],
                                              np.append(sps_t.final_section[sptides],
                                                        np.append(ups_t.final_section[uptides],fls_t.final_section[fltides]))))
        trans = np.append(trans,np.append(dws_t.final_transp[dwtides],
                                              np.append(sps_t.final_transp[sptides],
                                                        np.append(ups_t.final_transp[uptides],fls_t.final_transp[fltides]))))
        depth = np.append(depth,np.append(dws_t.final_depth[dwtides],
                                              np.append(sps_t.final_depth[sptides],
                                                        np.append(ups_t.final_depth[uptides],fls_t.final_depth[fltides]))))
        lon = np.append(lon,np.append(dws_t.final_lon[dwtides],
                                              np.append(sps_t.final_lon[sptides],
                                                        np.append(ups_t.final_lon[uptides],fls_t.final_lon[fltides]))))
        lat = np.append(lat,np.append(dws_t.final_lat[dwtides],
                                              np.append(sps_t.final_lat[sptides],
                                                        np.append(ups_t.final_lat[uptides],fls_t.final_lat[fltides]))))
        year = np.append(year,np.append([dwend[i].year] * len(dws_t.final_section[dwtides]),
                                              np.append([spend[i].year] * len(sps_t.final_section[sptides]),
                                                        np.append([upend[i].year] * len(ups_t.final_section[uptides]),[flend[i].year] * len(fls_t.final_section[fltides])))))
        salt = np.append(salt,np.append(dws_t.final_salt[dwtides],
                                              np.append(sps_t.final_salt[sptides],
                                                        np.append(ups_t.final_salt[uptides],fls_t.final_salt[fltides]))))
        temp = np.append(temp,np.append(dws_t.final_temp[dwtides],
                                              np.append(sps_t.final_temp[sptides],
                                                        np.append(ups_t.final_temp[uptides],fls_t.final_temp[fltides]))))
        nit = np.append(nit,np.append(dwdo_no3.final_salt[dwtides],
                                              np.append(spdo_no3.final_salt[sptides],
                                                        np.append(updo_no3.final_salt[uptides],fldo_no3.final_salt[fltides]))))


    d = {'year':year,  'section':section, 'depth':depth, 'lon':lon, 'lat':lat, 'transport':trans, 'salt':salt, 'temperature':temp, 'NO3':nit} #, 'DO': oxy, 'TA': ta, 'DIC':dic}
    df = pd.DataFrame(d)
    return df

all = get_tracers_all(dwendday,spendday,upendday,flendday)
all.to_csv('summary_files/combineddata.csv')
print('done')