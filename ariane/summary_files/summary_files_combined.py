import argparse
import xarray as xr
import pandas as pd
import numpy as np

# allow file selection to be input in the command line
# example run: python3 summary_files.py 20171012 up
parser = argparse.ArgumentParser(description='Get ERDAPP data.')
parser.add_argument('winter', type=str,
                    help='Path to winter files in the form YYYYMMDD')
parser.add_argument('spring', type=str,
                    help='Path to spring files in the form YYYYMMDD')
parser.add_argument('summer', type=str,
                    help='Path to summer files in the form YYYYMMDD')
parser.add_argument('fall', type=str,
                    help='Path to fall files in the form YYYYMMDD')
args = parser.parse_args()

fileDW = args.winter
fileUP = args.summer
fileS = args.spring
fileF = args.fall

# bring in files
Ust = xr.open_dataset('/data1/bbeutel/LO_user/ariane/up_cas7/S_T/{}/ariane_positions_quantitative.nc'.format(fileUP))
Udn = xr.open_dataset('/data1/bbeutel/LO_user/ariane/up_cas7/DO_NO3/{}/ariane_positions_quantitative.nc'.format(fileUP))
Utd = xr.open_dataset('/data1/bbeutel/LO_user/ariane/up_cas7/TA_DIC/{}/ariane_positions_quantitative.nc'.format(fileUP))
Dst = xr.open_dataset('/data1/bbeutel/LO_user/ariane/down_cas7/S_T/{}/ariane_positions_quantitative.nc'.format(fileDW))
Ddn = xr.open_dataset('/data1/bbeutel/LO_user/ariane/down_cas7/DO_NO3/{}/ariane_positions_quantitative.nc'.format(fileDW))
Dtd = xr.open_dataset('/data1/bbeutel/LO_user/ariane/down_cas7/TA_DIC/{}/ariane_positions_quantitative.nc'.format(fileDW))
Fst = xr.open_dataset('/data1/bbeutel/LO_user/ariane/buffer_cas7/Fall/{}/ariane_positions_quantitative.nc'.format(fileF))
Fdn = xr.open_dataset('/data1/bbeutel/LO_user/ariane/buffer_cas7/FallDONO3/{}/ariane_positions_quantitative.nc'.format(fileF))
Ftd = xr.open_dataset('/data1/bbeutel/LO_user/ariane/buffer_cas7/FallTADIC/{}/ariane_positions_quantitative.nc'.format(fileF))
Sst = xr.open_dataset('/data1/bbeutel/LO_user/ariane/buffer_cas7/Spring/{}/ariane_positions_quantitative.nc'.format(fileS))
Sdn = xr.open_dataset('/data1/bbeutel/LO_user/ariane/buffer_cas7/SpringDONO3/{}/ariane_positions_quantitative.nc'.format(fileS))
Std = xr.open_dataset('/data1/bbeutel/LO_user/ariane/buffer_cas7/SpringTADIC/{}/ariane_positions_quantitative.nc'.format(fileS))

# make dictionary of transport and tracer concentrations of each water parcel within a specified region
# the region is based on the boolean input into the function defined here
# region options are salish, cuc, offshore deep, offshore surface, north, south, fresh, loop

d = {'regions':['salish','cuc', 'offshore deep', 'offshore surface', 'north', 'south', 'fresh','loop'],
    'transport':np.zeros(8),'salt':np.zeros(8),'temp':np.zeros(8),'DO':np.zeros(8),
    'NO3':np.zeros(8),'TA':np.zeros(8),'DIC':np.zeros(8),'[TA-DIC]':np.zeros(8)}
df = pd.DataFrame(d)

# boundary definitions
bdy_loo = 0
bdy_sou = 2
bdy_off = 3
bdy_nor = 4
saltdiv = 32
sdiv = 33.5

# hours over which data was integrated
# start = 2401
# length =(np.max(s_t.init_t)-start+1) 

# total
Dstbool = ((abs(Dst.init_t-Dst.final_t) > 24) & ~np.isnan(Dst.final_section)) # total.. but not including tidal pumping or lost particles
Sstbool = ((abs(Sst.init_t-Sst.final_t) > 24) & ~np.isnan(Sst.final_section))
Ustbool = ((abs(Ust.init_t-Ust.final_t) > 24) & ~np.isnan(Ust.final_section))
Fstbool = ((abs(Fst.init_t-Fst.final_t) > 24) & ~np.isnan(Fst.final_section))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[0,'transport'] = np.sum(transport)
df.loc[0,'salt'] = np.average(np.append(Dst.init_salt[Dstbool].values, np.append(Sst.init_salt[Sstbool].values,np.append(Ust.init_salt[Ustbool],Fst.init_salt[Fstbool]))),weights=transport)
df.loc[0,'temp'] = np.average(np.append(Dst.init_temp[Dstbool].values, np.append(Sst.init_temp[Sstbool].values,np.append(Ust.init_temp[Ustbool],Fst.init_temp[Fstbool]))),weights=transport)
df.loc[0,'NO3'] = np.average(np.append(Ddn.init_salt[Dstbool].values, np.append(Sdn.init_salt[Sstbool].values,np.append(Udn.init_salt[Ustbool],Fdn.init_salt[Fstbool]))),weights=transport)
df.loc[0,'DO'] = np.average(np.append(Ddn.init_temp[Dstbool].values, np.append(Sdn.init_temp[Sstbool].values,np.append(Udn.init_temp[Ustbool],Fdn.init_temp[Fstbool]))),weights=transport)
df.loc[0,'DIC'] = np.average(np.append(Dtd.init_salt[Dstbool].values, np.append(Std.init_salt[Sstbool].values,np.append(Utd.init_salt[Ustbool],Ftd.init_salt[Fstbool]))),weights=transport)
df.loc[0,'TA'] = np.average(np.append(Dtd.init_temp[Dstbool].values, np.append(Std.init_temp[Sstbool].values,np.append(Utd.init_temp[Ustbool],Ftd.init_temp[Fstbool]))),weights=transport)
df.loc[0,'[TA-DIC]'] = np.average(np.append(Dtd.init_temp[Dstbool].values, np.append(Std.init_temp[Sstbool].values,np.append(Utd.init_temp[Ustbool],Ftd.init_temp[Fstbool])))
                                  -np.append(Dtd.init_salt[Dstbool].values, np.append(Std.init_salt[Sstbool].values,np.append(Utd.init_salt[Ustbool],Ftd.init_salt[Fstbool]))),weights=transport)

# for the rest of the water masses we use final properties, because we care about the properties of the water masses at they enter the region (before mixing with the other water masses)
# loop
Dstbool = ((abs(Dst.init_t-Dst.final_t) > 24) & (Dst.final_section==bdy_loo))
Sstbool = ((abs(Sst.init_t-Sst.final_t) > 24) & (Sst.final_section==bdy_loo))
Ustbool = ((abs(Ust.init_t-Ust.final_t) > 24) & (Ust.final_section==bdy_loo))
Fstbool = ((abs(Fst.init_t-Fst.final_t) > 24) & (Fst.final_section==bdy_loo))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[7,'transport'] = np.sum(transport)
df.loc[7,'salt'] = np.average(np.append(Dst.final_salt[Dstbool].values, np.append(Sst.final_salt[Sstbool].values,np.append(Ust.final_salt[Ustbool],Fst.final_salt[Fstbool]))),weights=transport)
df.loc[7,'temp'] = np.average(np.append(Dst.final_temp[Dstbool].values, np.append(Sst.final_temp[Sstbool].values,np.append(Ust.final_temp[Ustbool],Fst.final_temp[Fstbool]))),weights=transport)
df.loc[7,'NO3'] = np.average(np.append(Ddn.final_salt[Dstbool].values, np.append(Sdn.final_salt[Sstbool].values,np.append(Udn.final_salt[Ustbool],Fdn.final_salt[Fstbool]))),weights=transport)
df.loc[7,'DO'] = np.average(np.append(Ddn.final_temp[Dstbool].values, np.append(Sdn.final_temp[Sstbool].values,np.append(Udn.final_temp[Ustbool],Fdn.final_temp[Fstbool]))),weights=transport)
df.loc[7,'DIC'] = np.average(np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)
df.loc[7,'TA'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool]))),weights=transport)
df.loc[7,'[TA-DIC]'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool])))
                                  -np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)

# fresh
Dstbool = ((Dst.final_salt < saltdiv) & (Dst.final_section==bdy_sou))
Sstbool = ((Sst.final_salt < saltdiv) & (Sst.final_section==bdy_sou))
Ustbool = ((Ust.final_salt < saltdiv) & (Ust.final_section==bdy_sou))
Fstbool = ((Fst.final_salt < saltdiv) & (Fst.final_section==bdy_sou))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[6,'transport'] = np.sum(transport)
df.loc[6,'salt'] = np.average(np.append(Dst.final_salt[Dstbool].values, np.append(Sst.final_salt[Sstbool].values,np.append(Ust.final_salt[Ustbool],Fst.final_salt[Fstbool]))),weights=transport)
df.loc[6,'temp'] = np.average(np.append(Dst.final_temp[Dstbool].values, np.append(Sst.final_temp[Sstbool].values,np.append(Ust.final_temp[Ustbool],Fst.final_temp[Fstbool]))),weights=transport)
df.loc[6,'NO3'] = np.average(np.append(Ddn.final_salt[Dstbool].values, np.append(Sdn.final_salt[Sstbool].values,np.append(Udn.final_salt[Ustbool],Fdn.final_salt[Fstbool]))),weights=transport)
df.loc[6,'DO'] = np.average(np.append(Ddn.final_temp[Dstbool].values, np.append(Sdn.final_temp[Sstbool].values,np.append(Udn.final_temp[Ustbool],Fdn.final_temp[Fstbool]))),weights=transport)
df.loc[6,'DIC'] = np.average(np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)
df.loc[6,'TA'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool]))),weights=transport)
df.loc[6,'[TA-DIC]'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool])))
                                  -np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)


# south
Dstbool = ((Dst.final_salt>=saltdiv) & (Dst.final_salt < sdiv) & (Dst.final_section==bdy_sou))
Sstbool = ((Sst.final_salt>=saltdiv) & (Sst.final_salt < sdiv) & (Sst.final_section==bdy_sou))
Ustbool = ((Ust.final_salt>=saltdiv) & (Ust.final_salt < sdiv) & (Ust.final_section==bdy_sou))
Fstbool = ((Fst.final_salt>=saltdiv) & (Fst.final_salt < sdiv) & (Fst.final_section==bdy_sou))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[5,'transport'] = np.sum(transport)
df.loc[5,'salt'] = np.average(np.append(Dst.final_salt[Dstbool].values, np.append(Sst.final_salt[Sstbool].values,np.append(Ust.final_salt[Ustbool],Fst.final_salt[Fstbool]))),weights=transport)
df.loc[5,'temp'] = np.average(np.append(Dst.final_temp[Dstbool].values, np.append(Sst.final_temp[Sstbool].values,np.append(Ust.final_temp[Ustbool],Fst.final_temp[Fstbool]))),weights=transport)
df.loc[5,'NO3'] = np.average(np.append(Ddn.final_salt[Dstbool].values, np.append(Sdn.final_salt[Sstbool].values,np.append(Udn.final_salt[Ustbool],Fdn.final_salt[Fstbool]))),weights=transport)
df.loc[5,'DO'] = np.average(np.append(Ddn.final_temp[Dstbool].values, np.append(Sdn.final_temp[Sstbool].values,np.append(Udn.final_temp[Ustbool],Fdn.final_temp[Fstbool]))),weights=transport)
df.loc[5,'DIC'] = np.average(np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)
df.loc[5,'TA'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool]))),weights=transport)
df.loc[5,'[TA-DIC]'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool])))
                                  -np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)


# cuc
Dstbool = ((Dst.final_salt>=sdiv) & (Dst.final_section==bdy_sou))
Sstbool = ((Sst.final_salt>=sdiv) & (Sst.final_section==bdy_sou))
Ustbool = ((Ust.final_salt>=sdiv) & (Ust.final_section==bdy_sou))
Fstbool = ((Fst.final_salt>=sdiv) & (Fst.final_section==bdy_sou))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[1,'transport'] = np.sum(transport)
df.loc[1,'salt'] = np.average(np.append(Dst.final_salt[Dstbool].values, np.append(Sst.final_salt[Sstbool].values,np.append(Ust.final_salt[Ustbool],Fst.final_salt[Fstbool]))),weights=transport)
df.loc[1,'temp'] = np.average(np.append(Dst.final_temp[Dstbool].values, np.append(Sst.final_temp[Sstbool].values,np.append(Ust.final_temp[Ustbool],Fst.final_temp[Fstbool]))),weights=transport)
df.loc[1,'NO3'] = np.average(np.append(Ddn.final_salt[Dstbool].values, np.append(Sdn.final_salt[Sstbool].values,np.append(Udn.final_salt[Ustbool],Fdn.final_salt[Fstbool]))),weights=transport)
df.loc[1,'DO'] = np.average(np.append(Ddn.final_temp[Dstbool].values, np.append(Sdn.final_temp[Sstbool].values,np.append(Udn.final_temp[Ustbool],Fdn.final_temp[Fstbool]))),weights=transport)
df.loc[1,'DIC'] = np.average(np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)
df.loc[1,'TA'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool]))),weights=transport)
df.loc[1,'[TA-DIC]'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool])))
                                  -np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)


# off_d
Dstbool = ((Dst.final_depth>120) & (Dst.final_section==bdy_off))
Sstbool = ((Sst.final_depth>120) & (Sst.final_section==bdy_off))
Ustbool = ((Ust.final_depth>120) & (Ust.final_section==bdy_off))
Fstbool = ((Fst.final_depth>120) & (Fst.final_section==bdy_off))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[2,'transport'] = np.sum(transport)
df.loc[2,'salt'] = np.average(np.append(Dst.final_salt[Dstbool].values, np.append(Sst.final_salt[Sstbool].values,np.append(Ust.final_salt[Ustbool],Fst.final_salt[Fstbool]))),weights=transport)
df.loc[2,'temp'] = np.average(np.append(Dst.final_temp[Dstbool].values, np.append(Sst.final_temp[Sstbool].values,np.append(Ust.final_temp[Ustbool],Fst.final_temp[Fstbool]))),weights=transport)
df.loc[2,'NO3'] = np.average(np.append(Ddn.final_salt[Dstbool].values, np.append(Sdn.final_salt[Sstbool].values,np.append(Udn.final_salt[Ustbool],Fdn.final_salt[Fstbool]))),weights=transport)
df.loc[2,'DO'] = np.average(np.append(Ddn.final_temp[Dstbool].values, np.append(Sdn.final_temp[Sstbool].values,np.append(Udn.final_temp[Ustbool],Fdn.final_temp[Fstbool]))),weights=transport)
df.loc[2,'DIC'] = np.average(np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)
df.loc[2,'TA'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool]))),weights=transport)
df.loc[2,'[TA-DIC]'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool])))
                                  -np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)


# off_s
Dstbool = ((Dst.final_depth<=120) & (Dst.final_section==bdy_off))
Sstbool = ((Sst.final_depth<=120) & (Sst.final_section==bdy_off))
Ustbool = ((Ust.final_depth<=120) & (Ust.final_section==bdy_off))
Fstbool = ((Fst.final_depth<=120) & (Fst.final_section==bdy_off))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[3,'transport'] = np.sum(transport)
df.loc[3,'salt'] = np.average(np.append(Dst.final_salt[Dstbool].values, np.append(Sst.final_salt[Sstbool].values,np.append(Ust.final_salt[Ustbool],Fst.final_salt[Fstbool]))),weights=transport)
df.loc[3,'temp'] = np.average(np.append(Dst.final_temp[Dstbool].values, np.append(Sst.final_temp[Sstbool].values,np.append(Ust.final_temp[Ustbool],Fst.final_temp[Fstbool]))),weights=transport)
df.loc[3,'NO3'] = np.average(np.append(Ddn.final_salt[Dstbool].values, np.append(Sdn.final_salt[Sstbool].values,np.append(Udn.final_salt[Ustbool],Fdn.final_salt[Fstbool]))),weights=transport)
df.loc[3,'DO'] = np.average(np.append(Ddn.final_temp[Dstbool].values, np.append(Sdn.final_temp[Sstbool].values,np.append(Udn.final_temp[Ustbool],Fdn.final_temp[Fstbool]))),weights=transport)
df.loc[3,'DIC'] = np.average(np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)
df.loc[3,'TA'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool]))),weights=transport)
df.loc[3,'[TA-DIC]'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool])))
                                  -np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)


# north
Dstbool = ((Dst.final_section==bdy_nor))
Sstbool = ((Sst.final_section==bdy_nor))
Ustbool = ((Ust.final_section==bdy_nor))
Fstbool = ((Fst.final_section==bdy_nor))
transport = np.append(Dst.init_transp[Dstbool].values, np.append(Sst.init_transp[Sstbool].values,np.append(Ust.init_transp[Ustbool],Fst.init_transp[Fstbool])))
df.loc[4,'transport'] = np.sum(transport)
df.loc[4,'salt'] = np.average(np.append(Dst.final_salt[Dstbool].values, np.append(Sst.final_salt[Sstbool].values,np.append(Ust.final_salt[Ustbool],Fst.final_salt[Fstbool]))),weights=transport)
df.loc[4,'temp'] = np.average(np.append(Dst.final_temp[Dstbool].values, np.append(Sst.final_temp[Sstbool].values,np.append(Ust.final_temp[Ustbool],Fst.final_temp[Fstbool]))),weights=transport)
df.loc[4,'NO3'] = np.average(np.append(Ddn.final_salt[Dstbool].values, np.append(Sdn.final_salt[Sstbool].values,np.append(Udn.final_salt[Ustbool],Fdn.final_salt[Fstbool]))),weights=transport)
df.loc[4,'DO'] = np.average(np.append(Ddn.final_temp[Dstbool].values, np.append(Sdn.final_temp[Sstbool].values,np.append(Udn.final_temp[Ustbool],Fdn.final_temp[Fstbool]))),weights=transport)
df.loc[4,'DIC'] = np.average(np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)
df.loc[4,'TA'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool]))),weights=transport)
df.loc[4,'[TA-DIC]'] = np.average(np.append(Dtd.final_temp[Dstbool].values, np.append(Std.final_temp[Sstbool].values,np.append(Utd.final_temp[Ustbool],Ftd.final_temp[Fstbool])))
                                  -np.append(Dtd.final_salt[Dstbool].values, np.append(Std.final_salt[Sstbool].values,np.append(Utd.final_salt[Ustbool],Ftd.final_salt[Fstbool]))),weights=transport)


# save the file
filename = './combo_{}.csv'.format(fileUP[:4])
df.to_csv(filename)
print(filename)