import argparse
import xarray as xr
import pandas as pd
import numpy as np

# allow file selection to be input in the command line
# example run: python3 summary_files.py 20171012 up
parser = argparse.ArgumentParser(description='Get ERDAPP data.')
parser.add_argument('path', type=str,
                    help='Path to files in the form YYYYMMDD')
parser.add_argument('updown', type=str,
                    help='Upwelling or downwelling?')
args = parser.parse_args()

file = args.path
updown = args.updown

# bring in files
s_t = xr.open_dataset('/data1/bbeutel/LO_user/ariane/{}_cas7/S_T/{}/ariane_positions_quantitative.nc'.format(updown,file))
do_no3 = xr.open_dataset('/data1/bbeutel/LO_user/ariane/{}_cas7/DO_NO3/{}/ariane_positions_quantitative.nc'.format(updown,file))
ta_dic = xr.open_dataset('/data1/bbeutel/LO_user/ariane/{}_cas7/TA_DIC/{}/ariane_positions_quantitative.nc'.format(updown,file))

# make dictionary of transport and tracer concentrations of each water parcel within a specified region
# the region is based on the boolean input into the function defined here
# region options are salish, cuc, offshore deep, offshore surface, north, south, fresh, loop

regions = ['salish', 'cuc', 'offshore deep', 'offshore surface', 'north', 'south', 'fresh', 'loop']
transport = np.zeros(len(regions))
salt = np.zeros(len(regions))
temp = np.zeros(len(regions))
DO = np.zeros(len(regions))
NO3 = np.zeros(len(regions))
TA = np.zeros(len(regions))
DIC = np.zeros(len(regions))
# set up dictionary
d = {'regions':regions,
        'transport':transport,'salt':salt,'temp':temp,'DO':DO,'NO3':NO3,'TA':TA,'DIC':DIC}
df = pd.DataFrame(d)

# boundary definitions
bdy_loo = 0
bdy_sou = 2
bdy_off = 3
bdy_nor = 4
saltdiv = 32
sdiv = 33.5

# hours over which data was integrated
start = 2401
length =(np.max(s_t.init_t)-start+1) 

for i in range(len(regions)):
    region = regions[i]
    mydata= s_t

    if region == 'salish':
        boolean = ((abs(mydata.init_t-mydata.final_t) > 24) & ~np.isnan(mydata.final_section))
    elif region == 'cuc':
        boolean = (mydata.final_section==bdy_sou) & (mydata.final_salt >= sdiv)
    elif region == 'offshore deep':
        boolean = (mydata.final_section==bdy_off) & (mydata.final_depth > 120)
    elif region == 'offshore surface':
        boolean = (mydata.final_section==bdy_off) & (mydata.final_depth <=120)
    elif region == 'north':
        boolean = (mydata.final_section==bdy_nor)
    elif region == 'south':
        boolean = (mydata.final_section==bdy_sou) & (mydata.final_salt >= saltdiv) & (mydata.final_salt < sdiv)
    elif region == 'fresh':
        boolean = ((mydata.final_section==bdy_sou) & (mydata.final_salt < saltdiv))
    elif region == 'loop':
        boolean = (mydata.final_section==bdy_loo) & (abs(mydata.init_t-mydata.final_t) > 24)
    
    if region == 'salish': # for into JdF we're interested in the "initial" (ie. what it is going into JdF)
        # get data
        transport = mydata.init_transp[boolean].values
        df.loc[i,'transport'] = np.sum(transport)/length
        df.loc[i,'salt'] = np.average(mydata.init_salt[boolean].values, weights=transport)
        df.loc[i,'temp'] = np.average(mydata.init_temp[boolean].values, weights=transport)
        mydata = do_no3
        df.loc[i,'NO3'] = np.average(mydata.init_salt[boolean].values, weights=transport)
        df.loc[i,'DO'] = np.average(mydata.init_temp[boolean].values, weights=transport)
        # convert TA and DIC from mmol/m3 to umol/kg in this step too
        mydata = ta_dic
        df.loc[i,'DIC'] = np.average(mydata.init_salt[boolean].values/(s_t.init_dens[boolean].values+1000)*1000, weights=transport)
        df.loc[i,'TA'] = np.average(mydata.init_temp[boolean].values/(s_t.init_dens[boolean].values+1000)*1000, weights=transport)
    else: # for every other section we care about the "final" (ie. the conditions at the outer boundaries)
        # get data
        transport = mydata.final_transp[boolean].values
        df.loc[i,'transport'] = np.sum(transport)/length
        df.loc[i,'salt'] = np.average(mydata.final_salt[boolean].values, weights=transport)
        df.loc[i,'temp'] = np.average(mydata.final_temp[boolean].values, weights=transport)
        mydata = do_no3
        df.loc[i,'NO3'] = np.average(mydata.final_salt[boolean].values, weights=transport)
        df.loc[i,'DO'] = np.average(mydata.final_temp[boolean].values, weights=transport)
        # convert TA and DIC from mmol/m3 to umol/kg in this step too
        mydata = ta_dic
        df.loc[i,'DIC'] = np.average(mydata.final_salt[boolean].values/(s_t.final_dens[boolean].values+1000)*1000, weights=transport)
        df.loc[i,'TA'] = np.average(mydata.final_temp[boolean].values/(s_t.final_dens[boolean].values+1000)*1000, weights=transport)

# save the file
filename = './{}_{}.csv'.format(updown,file[:4])
df.to_csv(filename)
print(filename)