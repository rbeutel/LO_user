# make csv of each water parcel, its WM source, transport, and isopycnal that it lies on
# at the beginning of its trajectory 
import xarray as xr
import numpy as np
import pandas as pd
import gsw

def isopycnaldata(data_list):
    # for the purpose of seeing which isopycnals get in, only thinking about pacific sources (ie. not return flow)

    trans = np.array([])
    section = np.array([])
    salt = np.array([])
    depth = np.array([])
    temp = np.array([])

    for datapath in data_list:
        data = xr.open_dataset(datapath)
        rebool = (~np.isnan(data.final_section) & (data.final_section != 0))
        salt = np.append(salt,data.final_salt[rebool].values)
        temp = np.append(temp,data.final_temp[rebool].values)
        depth = np.append(depth,data.final_depth[rebool].values)
        section = np.append(section,data.final_section[rebool].values)
        trans = np.append(trans,data.final_transp[rebool].values)

    d = {'transport':trans,'section':section,'salt':salt,'temp':temp,'depth':depth}
    df = pd.DataFrame(d)

    return df

file_list = ['/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20131024/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20140903/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20150905/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20160913/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20171012/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20180906/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20191105/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20201017/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20210922/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20221015/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/up_cas7/S_T/20230922/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20140306/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20150212/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20160319/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20170419/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20180201/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20190406/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20200127/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20210202/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20220125/ariane_positions_quantitative.nc',
'/data1/bbeutel/LO_user/ariane/down_cas7/S_T/20230419/ariane_positions_quantitative.nc']

data = isopycnaldata(file_list)

data['isopycnal'] = gsw.density.sigma0(data.salt.values,gsw.conversions.CT_from_t(data.salt.values,data.temp.values,data.depth.values))

# assign each line to a particular water mass
data['wm'] = 'none'

# boundary definitions
bdy_loo = 0
bdy_sou = 2
bdy_off = 3
bdy_nor = 4
saltdiv = 32
sdiv = 33.5

for i in range(len(data)):
    if (data.section[i]==bdy_sou) and (data.salt[i] >= sdiv):
        data.wm[i]='cuc'
    elif (data.section[i]==bdy_off) & (data.depth[i] > 120):
        data.wm[i]='offshore deep'
    elif (data.section[i]==bdy_off) & (data.depth[i] <=120):
        data.wm[i]= 'offshore surface'
    elif (data.section[i]==bdy_nor):
        data.wm[i]= 'north'
    elif (data.section[i]==bdy_sou) & (data.salt[i] >= saltdiv) & (data.salt[i] < sdiv):
        data.wm[i]= 'south'
    elif ((data.section[i]==bdy_sou) & (data.salt[i] < saltdiv)):
        data.wm[i]= 'fresh'

data.to_csv('/data1/bbeutel/LO_user/ariane/summary_files/isopycnals.csv')