# conda activate EEA

# Libreries
import os
import sys

import numpy as np
import pandas as pd
import argparse
import math
from datetime import datetime, timedelta
import airbase
import json
import gc

import warnings
import asyncio

import platform
if platform.system()=='Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

warnings.filterwarnings("ignore")

def joinpath(rootdir, targetdir):
    return os.path.join(os.sep, rootdir + os.sep, targetdir)

# Path of EEA_data
path_main_dir_EEA_data = os.environ['EEA_data']

if path_main_dir_EEA_data == "":
    print("Error: set the environmental variables of EEA data")
    exit(-1)

# Function related on creation of empty Italy dictionary
def empty_dict_italy_region():

    list_italy_region = [   "All_regions", "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", \
                            "Friuli Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", \
                            "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", \
                            "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"
                    ]
    
    dict_italy_region = {}

    for region in list_italy_region:
        
        dict_current_region = {}
        dict_current_region["N_stations"] = 0
        dict_current_region["stations"] = {}

        # Total number of measures
        dict_current_region["N_tot_measures"] = 0

        # Total number of valid measures
        dict_current_region["N_tot_valid_measures"] = 0

        # Total number of missing values
        dict_current_region["N_tot_missing_values"] = 0

        # Sorted list of the stations with minumin number of
        # missing values in the maximum lenght of missing data tract
        dict_current_region["sorted_station_MDTs_min"] = []

        # Number of station with more than 75 % of valid samples
        dict_current_region["N_stations_more_75_perc_of_valid_data"] = 0

        # Type of EEA station
        dict_current_region["N_station_AirQualityStationType"] = {  
                                                                    "background": 0, \
                                                                    "industrial": 0, \
                                                                    "traffic": 0
                                                                }

        # Type of area where it is located the EEA station
        dict_current_region["N_station_AirQualityStationArea"] = {
                                                            "rural": 0, \
                                                            "rural-nearcity": 0, \
                                                            "rural-regional": 0, \
                                                            "rural-remote": 0,
                                                            "suburban": 0,
                                                            "urban": 0,
                                                        }
        
        dict_italy_region[region] = dict_current_region

    return dict_italy_region

# Function related on creation of empty Italy dictionary for each type of station
def empty_dict_italy_region_air_station_type(AirQualityStationType):

    list_italy_region = [   "All_regions", "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", \
                            "Friuli Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", \
                            "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", \
                            "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"
                    ]
    
    dict_italy_region = {}

    for region in list_italy_region:
        
        dict_current_region = {}
        dict_current_region["AirQualityStationType"] = AirQualityStationType
        dict_current_region["N_stations"] = 0
        dict_current_region["stations"] = {}

        # Total number of measures
        dict_current_region["N_tot_measures"] = 0

        # Total number of valid measures
        dict_current_region["N_tot_valid_measures"] = 0

        # Total number of missing values
        dict_current_region["N_tot_missing_values"] = 0

        # Sorted list of the stations with minumin number of
        # missing values in the maximum lenght of missing data tract
        dict_current_region["sorted_station_MDTs_min"] = []

        # Number of station with more than 75 % of valid samples
        dict_current_region["N_stations_more_75_perc_of_valid_data"] = 0

        # Type of area where it is located the EEA station
        dict_current_region["N_station_AirQualityStationArea"] = {
                                                            "rural": 0, \
                                                            "rural-nearcity": 0, \
                                                            "rural-regional": 0, \
                                                            "rural-remote": 0,
                                                            "suburban": 0,
                                                            "urban": 0,
                                                        }
        
        dict_italy_region[region] = dict_current_region

    return dict_italy_region

# Function related on creation of empty Italy dictionary for each type of area
def empty_dict_italy_region_air_station_area(AirQualityStationArea):

    list_italy_region = [   "All_regions", "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", \
                            "Friuli Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", \
                            "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", \
                            "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"
                    ]
    
    dict_italy_region = {}

    for region in list_italy_region:
        
        dict_current_region = {}
        dict_current_region["AirQualityStationArea"] = AirQualityStationArea
        dict_current_region["N_stations"] = 0
        dict_current_region["stations"] = {}

        # Total number of measures
        dict_current_region["N_tot_measures"] = 0

        # Total number of valid measures
        dict_current_region["N_tot_valid_measures"] = 0

        # Total number of missing values
        dict_current_region["N_tot_missing_values"] = 0

        # Sorted list of the stations with minumin number of
        # missing values in the maximum lenght of missing data tract
        dict_current_region["sorted_station_MDTs_min"] = []

        # Number of station with more than 75 % of valid samples
        dict_current_region["N_stations_more_75_perc_of_valid_data"] = 0

        # Type of station located in the area observed
        dict_current_region["N_station_AirQualityStationType"] = {
                                                                    "background" : 0, \
                                                                    "traffic": 0, \
                                                                    "industrial": 0
                                                                }
        
        dict_italy_region[region] = dict_current_region

    return dict_italy_region

def fill_dict_station(  
                        Number_of_MDTs, Max_len_of_MDTs, Min_len_of_MDTs,
                        Avg_len_of_MDTs, N_tot_measures, N_tot_valid_measures,
                        N_tot_missing_values, AirQualityStationType, AirQualityStationArea
                    ):

    dict_info_station = {}

    # Total number of Missing Data Tracts
    dict_info_station["Number_of_MDTs"] = Number_of_MDTs

    # Maximum lenght of Missing Data Tracts
    dict_info_station["Max_len_of_MDTs"] = Max_len_of_MDTs

    # Minimum lenght of Missing Data Tracts
    dict_info_station["Min_len_of_MDTs"] = Min_len_of_MDTs

    # Average lenght of Missing Data Tracts
    dict_info_station["Avg_len_of_MDTs"] = Avg_len_of_MDTs

    # Total number of measures
    dict_info_station["N_tot_measures"] = N_tot_measures

    # Total number of valid measures
    dict_info_station["N_tot_valid_measures"] = N_tot_valid_measures

    # Total number of missing values
    dict_info_station["N_tot_missing_values"] = N_tot_missing_values

    # Percentage of valid samples
    dict_info_station["Percentage_of_valid_data"] = (
                                                        (N_tot_valid_measures / float(N_tot_measures)) * 100 if \
                                                        N_tot_measures > 0 else 0.0
                                                    )

    # Type of EEA station
    dict_info_station["AirQualityStationType"] = AirQualityStationType

    # Type of area where it is located the EEA station
    dict_info_station["AirQualityStationArea"] = AirQualityStationArea

    return dict_info_station

# Client of Airbase
client = airbase.AirbaseClient()

list_air_pollutant = ["CO", "NO2", "O3", "PM2.5", "PM10", "SO2"]

# Dictionary of limit threshoulds of air pollutants, expressed
# in μg/m^3, used for checking if the measurements of EEA station are valid
dict_limit_threshould_air_pollutant = {}

dict_limit_threshould_air_pollutant["CO"] = 15000
dict_limit_threshould_air_pollutant["NO2"] = 700
dict_limit_threshould_air_pollutant["O3"] = 500
dict_limit_threshould_air_pollutant["PM10"] = 1000
dict_limit_threshould_air_pollutant["PM2.5"] = 700
dict_limit_threshould_air_pollutant["SO2"] = 1200

list_countries = client.all_countries

parser = argparse.ArgumentParser(description='Convert text information info Json file of EEA stations')
parser.add_argument('-a', '--air_pollutant', help='Air Pollutant CO - NO2 - O3 - PM2.5 - PM10 - SO2', choices=list_air_pollutant, required=True)
parser.add_argument('-c', '--country', help='Country to observe', choices=list_countries, required=True)
parser.add_argument('-freq_mode', '--freq_mode', help='Frequency mode (hour, day)', type=str, required=True)
parser.add_argument('-start_year', '--start_year', help='Start year to acquire', type=int, required=True)
parser.add_argument('-end_year', '--end_year', help='End year to acquire', type=int, required=True)
args = vars(parser.parse_args())

air_poll_selected = args["air_pollutant"]
country = args["country"]
freq_mode = args["freq_mode"]
start_year = int(args["start_year"])
end_year = int(args["end_year"])

if start_year > end_year:
   print("End_year must be greater then start_year!")
   exit(-1)

# Path of metainfo files
path_metainfo_file = joinpath(path_main_dir_EEA_data, "New_metainfo")
path_metainfo_file = joinpath(path_metainfo_file, air_poll_selected + "_" + country + "_metainfo.csv")

# Path of CSV EEA stations data
path_dir_csv = joinpath(path_main_dir_EEA_data, "EEA_data")
path_dir_csv = joinpath(path_dir_csv, country)
path_dir_csv = joinpath(path_dir_csv, air_poll_selected)
path_dir_csv = joinpath(path_dir_csv, freq_mode)

# Find CSV data file
list_files_dir_air_pol_csv = os.listdir(path_dir_csv)
path_file_air_pol_data_csv = None

for file in list_files_dir_air_pol_csv:
    if file.endswith('.csv'):
        path_file_air_pol_data_csv = joinpath(path_dir_csv, file)
        break

# Read CSV file of the air pollutant specified
	
if air_poll_selected == "NO2":
  low_memory_flag = True
else:
  low_memory_flag = False

df_air_pol_data = pd.read_table(    path_file_air_pol_data_csv, delimiter=',', 
                                    header=[0], index_col=0, low_memory=low_memory_flag
                                )

# Read metainfo file about the air pollutant and country defined
df_air_pol_metainfo = pd.read_table(path_metainfo_file, delimiter=',', index_col=0)

current_year = start_year

# For each year asked
for idx_year in range(start_year,end_year):

    print("Current year: ", str(current_year))

    start_date_current_year = datetime(current_year, 1, 1, 0, 0)

    if freq_mode == "hour":
        delta = 1
    else:
        delta = 24
    
    # Read text file of current year analyzed
    with open(joinpath(path_dir_csv, str(current_year) + ".txt")) as f:
        lines_current_year = f.readlines()

    if country == "IT":
        dict_country_current_year = empty_dict_italy_region()

        dict_country_current_year_background = empty_dict_italy_region_air_station_type("background")
        dict_country_current_year_industrial = empty_dict_italy_region_air_station_type("industrial")
        dict_country_current_year_traffic = empty_dict_italy_region_air_station_type("traffic")

        dict_country_current_year_rural = empty_dict_italy_region_air_station_area("rural")
        dict_country_current_year_rural_nearcity = empty_dict_italy_region_air_station_area("rural-nearcity")
        dict_country_current_year_rural_regional = empty_dict_italy_region_air_station_area("rural-regional")
        dict_country_current_year_rural_remote = empty_dict_italy_region_air_station_area("rural-remote")
        dict_country_current_year_suburban = empty_dict_italy_region_air_station_area("suburban")
        dict_country_current_year_urban = empty_dict_italy_region_air_station_area("urban")
    else:
        print("Implement for the country specified")
        exit(-1)

    # For all station of current year
    for cod_station in lines_current_year:

        # Check about the validity of cod_station
        if country in cod_station:
            
            cod_station = cod_station.replace("\n","")

            print("Current code station: ", cod_station)

            # Query on metainfo due to cod_station
            df_current_station = df_air_pol_metainfo.filter(like=cod_station, axis=0)

            current_station_region = df_current_station["Regione"].values[0]
            AirQualityStationType_current_station = df_current_station['AirQualityStationType'].values[0]
            AirQualityStationArea_current_station = df_current_station['AirQualityStationArea'].values[0]

            # --------------------- Computing Missing values --------------------- 

            # Query on csv file due to cod_station
            df_current_station = df_air_pol_data.filter(like=cod_station, axis=0)
            
            # Sorting by DatetimeBegin
            df_current_station = df_current_station.sort_values(by='DatetimeBegin')

            # Inizialization of counting variables of current station
            count_total_measures_current_station_current_year = 0
            count_valid_measures_current_station_current_year = 0
            count_nan_measures_current_station_current_year = 0
            len_max_missing_values_current_station_current_year = 0
            len_min_missing_values_current_station_current_year = np.inf
            len_avg_missing_values_current_station_current_year = 0
            count_missing_data_tracts_current_station_current_year = 0

            # If it has been observed a new Missing Data Tract
            current_MDT = False
            current_len_MDT = 0
            
            first_day = True

            # Retrive measures of current station about the current year considered
            start_date_current_year = str(current_year) + "-01-01"
            end_date_current_year = str(current_year) + "-12-31"
            
            start_date_current_year_date = datetime(current_year, 1, 1, 0, 0)

            mask =  (df_current_station['DatetimeBegin'] >= start_date_current_year) & \
                    (df_current_station['DatetimeBegin'] <= end_date_current_year)
    
            df_current_station_current_year = df_current_station.loc[mask]
        
            for idx, row in df_current_station_current_year.iterrows():
                
                # It it is the first measure
                if first_day:
                    first_date_current_station = \
                        datetime.strptime(row["DatetimeBegin"].replace(' +01:00', ''), '%Y-%m-%d %H:%M:%S')

                    diff_dates = first_date_current_station - start_date_current_year_date
                    diff_dates = int(diff_dates.total_seconds() / (60*60*delta))

                    # Check if the first measure is acquired after then current_year-01-01 00:00:00
                    if diff_dates > 0:
                        count_missing_data_tracts_current_station_current_year += 1
                        len_max_missing_values_current_station_current_year = diff_dates
                        len_min_missing_values_current_station_current_year = diff_dates
                        len_avg_missing_values_current_station_current_year = diff_dates
                    
                    first_day = False

                count_total_measures_current_station_current_year += 1

                # Check if the measure is a missing value
                if  math.isnan(row["Concentration"]) or row["Concentration"] < 0.0 or \
                    row["Concentration"] > dict_limit_threshould_air_pollutant[air_poll_selected]:

                    df_current_station_current_year.at[idx, "Concentration"] = np.nan
                        
                    count_nan_measures_current_station_current_year += 1
                    current_len_MDT += 1

                    # If it is the first measure of a new Missing Data Tract
                    if current_MDT == False:
                        count_missing_data_tracts_current_station_current_year += 1
                        current_MDT = True

                # Instead, it is a valid measure
                else:
                    count_valid_measures_current_station_current_year += 1

                    # If it is the first measure after a Missing Data Tract
                    if current_MDT == True:

                        len_avg_missing_values_current_station_current_year += current_len_MDT

                        # Update information about the previous Missing Data Tract
                        if len_max_missing_values_current_station_current_year < current_len_MDT:
                            len_max_missing_values_current_station_current_year = current_len_MDT

                        if len_min_missing_values_current_station_current_year > current_len_MDT:
                            len_min_missing_values_current_station_current_year = current_len_MDT

                        current_len_MDT = 0
                        current_MDT = False

            # Compute the average lenght of Missing Data Tract
            if count_missing_data_tracts_current_station_current_year > 0:
                len_avg_missing_values_current_station_current_year /= float(count_missing_data_tracts_current_station_current_year)
            
            dict_country_current_year["All_regions"]["N_stations"] += 1
            dict_country_current_year[current_station_region]["N_stations"] += 1

            dict_country_current_year[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
            dict_country_current_year[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
            dict_country_current_year[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

            dict_country_current_year["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
            dict_country_current_year["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
            dict_country_current_year["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year
            
            dict_info_current_station = fill_dict_station(  
                                                            count_missing_data_tracts_current_station_current_year, 
                                                            len_max_missing_values_current_station_current_year, 
                                                            len_min_missing_values_current_station_current_year,
                                                            len_avg_missing_values_current_station_current_year, 
                                                            count_total_measures_current_station_current_year, 
                                                            count_valid_measures_current_station_current_year,
                                                            count_nan_measures_current_station_current_year,
                                                            AirQualityStationType_current_station,
                                                            AirQualityStationArea_current_station
                                                    )

            dict_country_current_year[current_station_region]["stations"][cod_station] = dict_info_current_station

            if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                dict_country_current_year[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                dict_country_current_year["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1
            
            dict_country_current_year[current_station_region]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
            dict_country_current_year[current_station_region]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1

            dict_country_current_year["All_regions"]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
            dict_country_current_year["All_regions"]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1

            # Sorting the dictionary due to the minumum lenght of the maximum number of
            # missing values of all stations
            dict_tmp = dict_country_current_year[current_station_region]["stations"].copy()
            list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

            dict_country_current_year[current_station_region]["sorted_station_MDTs_min"] = []
            for k in range(len(list_sorted)):
                dict_country_current_year[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])
            
            if AirQualityStationType_current_station == "background":
                dict_country_current_year_background["All_regions"]["N_stations"] += 1
                dict_country_current_year_background[current_station_region]["N_stations"] += 1

                dict_country_current_year_background["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_background["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_background["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_background[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_background[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_background[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_background[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_background[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_background["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_background[current_station_region]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1
                dict_country_current_year_background["All_regions"]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_background[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_background[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_background[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            elif AirQualityStationType_current_station == "industrial":
                dict_country_current_year_industrial["All_regions"]["N_stations"] += 1
                dict_country_current_year_industrial[current_station_region]["N_stations"] += 1

                dict_country_current_year_industrial["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_industrial["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_industrial["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_industrial[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_industrial[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_industrial[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_industrial[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_industrial[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_industrial["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_industrial[current_station_region]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1
                dict_country_current_year_industrial["All_regions"]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_industrial[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_industrial[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_industrial[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            else:
                dict_country_current_year_traffic["All_regions"]["N_stations"] += 1
                dict_country_current_year_traffic[current_station_region]["N_stations"] += 1

                dict_country_current_year_traffic["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_traffic["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_traffic["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_traffic[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_traffic[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_traffic[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_traffic[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_traffic[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_traffic["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_traffic[current_station_region]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1
                dict_country_current_year_traffic["All_regions"]["N_station_AirQualityStationArea"][AirQualityStationArea_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_traffic[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_traffic[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_traffic[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            if AirQualityStationArea_current_station == "rural":
                dict_country_current_year_rural["All_regions"]["N_stations"] += 1
                dict_country_current_year_rural[current_station_region]["N_stations"] += 1

                dict_country_current_year_rural["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_rural[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_rural["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_rural[current_station_region]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
                dict_country_current_year_rural["All_regions"]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_rural[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_rural[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_rural[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            elif AirQualityStationArea_current_station == "rural-nearcity":
                dict_country_current_year_rural_nearcity["All_regions"]["N_stations"] += 1
                dict_country_current_year_rural_nearcity[current_station_region]["N_stations"] += 1

                dict_country_current_year_rural_nearcity["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural_nearcity["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural_nearcity["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural_nearcity[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural_nearcity[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural_nearcity[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural_nearcity[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_rural_nearcity[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_rural_nearcity["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_rural_nearcity[current_station_region]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
                dict_country_current_year_rural_nearcity["All_regions"]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_rural_nearcity[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_rural_nearcity[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_rural_nearcity[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            elif AirQualityStationArea_current_station == "rural-regional":
                dict_country_current_year_rural_regional["All_regions"]["N_stations"] += 1
                dict_country_current_year_rural_regional[current_station_region]["N_stations"] += 1

                dict_country_current_year_rural_regional["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural_regional["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural_regional["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural_regional[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural_regional[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural_regional[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural_regional[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_rural_regional[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_rural_regional["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_rural_regional[current_station_region]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
                dict_country_current_year_rural_regional["All_regions"]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_rural_regional[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_rural_regional[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_rural_regional[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            elif AirQualityStationArea_current_station == "rural-remote":
                dict_country_current_year_rural_remote["All_regions"]["N_stations"] += 1
                dict_country_current_year_rural_remote[current_station_region]["N_stations"] += 1

                dict_country_current_year_rural_remote["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural_remote["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural_remote["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural_remote[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_rural_remote[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_rural_remote[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_rural_remote[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_rural_remote[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_rural_remote["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_rural_remote[current_station_region]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
                dict_country_current_year_rural_remote["All_regions"]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_rural_remote[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_rural_remote[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_rural_remote[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            elif AirQualityStationArea_current_station == "suburban":
                dict_country_current_year_suburban["All_regions"]["N_stations"] += 1
                dict_country_current_year_suburban[current_station_region]["N_stations"] += 1

                dict_country_current_year_suburban["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_suburban["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_suburban["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_suburban[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_suburban[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_suburban[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_suburban[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_suburban[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_suburban["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_suburban[current_station_region]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
                dict_country_current_year_suburban["All_regions"]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_suburban[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_suburban[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_suburban[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

            else:
                dict_country_current_year_urban["All_regions"]["N_stations"] += 1
                dict_country_current_year_urban[current_station_region]["N_stations"] += 1

                dict_country_current_year_urban["All_regions"]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_urban["All_regions"]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_urban["All_regions"]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_urban[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
                dict_country_current_year_urban[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
                dict_country_current_year_urban[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

                dict_country_current_year_urban[current_station_region]["stations"][cod_station] = dict_info_current_station

                if dict_info_current_station["Percentage_of_valid_data"] >= 75.0:
                    dict_country_current_year_urban[current_station_region]["N_stations_more_75_perc_of_valid_data"] += 1
                    dict_country_current_year_urban["All_regions"]["N_stations_more_75_perc_of_valid_data"] += 1

                dict_country_current_year_urban[current_station_region]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1
                dict_country_current_year_urban["All_regions"]["N_station_AirQualityStationType"][AirQualityStationType_current_station] += 1

                # Sorting the dictionary due to the minumum lenght of the maximum number of
                # missing values of all stations
                dict_tmp = dict_country_current_year_urban[current_station_region]["stations"].copy()
                list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

                dict_country_current_year_urban[current_station_region]["sorted_station_MDTs_min"] = []
                for k in range(len(list_sorted)):
                    dict_country_current_year_urban[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

    # Save the dictionary in a JSON file
    path_current_year_json = joinpath(path_dir_csv, str(current_year) + ".json")

    path_current_year_background_json = joinpath(path_dir_csv, str(current_year) + "_background.json")
    path_current_year_industrial_json = joinpath(path_dir_csv, str(current_year) + "_industrial.json")
    path_current_year_traffic_json = joinpath(path_dir_csv, str(current_year) + "_traffic.json")

    path_current_year_rural_json = joinpath(path_dir_csv, str(current_year) + "_rural.json")
    path_current_year_rural_nearcity_json = joinpath(path_dir_csv, str(current_year) + "_rural_nearcity.json")
    path_current_year_rural_regional_json = joinpath(path_dir_csv, str(current_year) + "_rural_regional.json")
    path_current_year_rural_remote_json = joinpath(path_dir_csv, str(current_year) + "_rural_remote.json")
    path_current_year_suburban_json = joinpath(path_dir_csv, str(current_year) + "_suburban.json")
    path_current_year_urban_json = joinpath(path_dir_csv, str(current_year) + "_urban.json")
    
    with open(path_current_year_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year, indent=4)
        json_file.write(json_dumps_str)

    with open(path_current_year_background_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_background, indent=4)
        json_file.write(json_dumps_str)
    
    with open(path_current_year_industrial_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_industrial, indent=4)
        json_file.write(json_dumps_str)

    with open(path_current_year_traffic_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_traffic, indent=4)
        json_file.write(json_dumps_str)

    with open(path_current_year_rural_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_rural, indent=4)
        json_file.write(json_dumps_str)
    
    with open(path_current_year_rural_nearcity_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_rural_nearcity, indent=4)
        json_file.write(json_dumps_str)
    
    with open(path_current_year_rural_regional_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_rural_regional, indent=4)
        json_file.write(json_dumps_str)
    
    with open(path_current_year_rural_remote_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_rural_remote, indent=4)
        json_file.write(json_dumps_str)
    
    with open(path_current_year_suburban_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_suburban, indent=4)
        json_file.write(json_dumps_str)
    
    with open(path_current_year_urban_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year_urban, indent=4)
        json_file.write(json_dumps_str)

    current_year += 1

df_air_pol_data.to_csv(path_file_air_pol_data_csv, sep=',')