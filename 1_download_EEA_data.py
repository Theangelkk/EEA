# conda activate EEA

# Libreries
import sys
import os
import numpy as np
import pandas as pd
import airbase
import argparse
import warnings
import asyncio

import platform
if platform.system()=='Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

warnings.filterwarnings("ignore")

# Path of EEA_data
path_main_dir_EEA_data = os.environ['EEA_data']

if path_main_dir_EEA_data == "":
    print("Error: set the environmental variables of EEA data")
    exit(-1)

if not os.path.exists(path_main_dir_EEA_data):
  os.mkdir(path_main_dir_EEA_data)

def joinpath(rootdir, targetdir):
    return os.path.join(os.sep, rootdir + os.sep, targetdir)

# Client Airbase
client = airbase.AirbaseClient()

list_of_air_poll = ["CO", "NO2", "O3", "PM2.5", "PM10", "SO2"]
list_of_countries = client.all_countries
list_of_freq_mode = ["hour", "day"]

parser = argparse.ArgumentParser(description='Script for downloading EEA stations')
parser.add_argument('-a','--air_poll', help='Air pollutant [CO,NO2,O3,PM2.5,PM10,SO2]', choices=list_of_air_poll,\
                    required=True)
parser.add_argument('-c','--country', help='Country selected', choices=list_of_countries,\
                    required=True)
parser.add_argument('-start_year','--start_year', help='Start year of download', type=int, required=True)
parser.add_argument('-end_year','--end_year', help='End year of download', type=int, required=True)
parser.add_argument('-freq_mode','--freq_mode', help='Frequency mode [hour,day]', choices=list_of_freq_mode, \
                     required=True)
args = vars(parser.parse_args())

air_poll_selected = args["air_poll"]
country = args["country"]
start_year = int(args["start_year"])
end_year = int(args["end_year"])
freq_mode = args["freq_mode"]

if start_year > end_year:
   print("End_year must be greater then start_year!")
   exit(-1)

# Path of metainfo file
dir_metainfo = joinpath(os.getcwd(), "New_metainfo")
path_metainfo = joinpath(dir_metainfo, air_poll_selected + "_" + country + "_metainfo.csv")

# Request of air pollutant to download
req_air_pol = client.request(country=[country], pl=[air_poll_selected], year_from=start_year, year_to=end_year)

path_main_dir_EEA_data = joinpath(path_main_dir_EEA_data, "EEA_data")

if not os.path.exists(path_main_dir_EEA_data):
  os.mkdir(path_main_dir_EEA_data)

dir_country = joinpath(path_main_dir_EEA_data, country)

if not os.path.exists(dir_country):
  os.mkdir(dir_country)

dir_air_pol = joinpath(dir_country, air_poll_selected)

if not os.path.exists(dir_air_pol):
  os.mkdir(dir_air_pol)

path_raw_data = joinpath(dir_air_pol, country + "_" + air_poll_selected + "_" + str(start_year) + "_" + str(end_year) + ".csv")

# Download Raw CSV file of air pollutant in the time interval defined
if not os.path.exists(path_raw_data):
    req_air_pol.download_to_file(path_raw_data)

# Loading Raw CSV file -- Index 4: AirQualityStationEoICode	
if air_poll_selected == "NO2":
  low_memory_flag = True
else:
  low_memory_flag = False

df = pd.read_table( path_raw_data, delimiter=',', header=[0], index_col=4, low_memory=low_memory_flag, on_bad_lines='skip', \
                    encoding='unicode_escape')

# Filtering due to freq_mode specified
df_freq_mode = df.loc[df['AveragingTime'] == freq_mode]

# Selecting interested columns
df_freq_mode = df_freq_mode.filter(items=['Concentration','DatetimeBegin'])

# Reset DataFrame with columns in desired order
df_freq_mode = df_freq_mode[['DatetimeBegin','Concentration']]

dir_air_pol_freq_mode = joinpath(dir_air_pol, freq_mode)

if not os.path.exists(dir_air_pol_freq_mode):
  os.mkdir(dir_air_pol_freq_mode)

path_ds_freq_mode = joinpath(dir_air_pol_freq_mode, country + "_" + air_poll_selected + "_" + str(start_year) + "_" + str(end_year) + "_" + freq_mode + ".csv")

if not os.path.exists(path_ds_freq_mode):
    # Sorting by DatetimeBegin
    df_freq_mode_date = df_freq_mode.sort_values(by='DatetimeBegin')
    df_freq_mode_date.to_csv(path_ds_freq_mode)

path_log = joinpath(dir_air_pol_freq_mode, country + "_" + air_poll_selected + "_" + str(start_year) + "_" + str(end_year) + "_" + freq_mode + "_log.txt")
string_output = "Number of " + air_poll_selected + " stations: " + str(len(df_freq_mode.index.unique()))

print(string_output)

with open(path_log, "a") as file_log:
  file_log.write(string_output + "\n")

# Splitting for each year
current_year = start_year

for i in range(start_year, end_year):

    print("Current year: " + str(current_year))
    start_date_current_year = str(current_year) + "-01-01"
    end_date_current_year = str(current_year) + "-12-31"

    path_file_current_year = joinpath(dir_air_pol_freq_mode, str(current_year) + ".txt")

    mask =  (df_freq_mode['DatetimeBegin'] >= start_date_current_year) & \
            (df_freq_mode['DatetimeBegin'] <= end_date_current_year)
    
    df_freq_mode_current_year = df_freq_mode.loc[mask]

    string_output = "Number of " + air_poll_selected + " stations year " + str(current_year) + ": " + \
              str(len(df_freq_mode_current_year.index.dropna().unique()))
    
    print(string_output)

    with open(path_log, "a") as file_log:
      file_log.write(string_output + "\n")

    list_idx_current_year = df_freq_mode_current_year.index.dropna().unique()

    for current_station in list_idx_current_year:

      with open(path_file_current_year, "a") as file_current_year:
        file_current_year.write(current_station + "\n")

    current_year += 1