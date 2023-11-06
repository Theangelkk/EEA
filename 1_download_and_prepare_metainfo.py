# conda activate EEA

# Libreries
import sys
import os
import numpy as np
import pandas as pd
import airbase
import matplotlib.pyplot as plt
import argparse
import geopandas as gpd
from shapely.geometry import Point, Polygon
import nest_asyncio

import warnings

warnings.filterwarnings("ignore")
nest_asyncio.apply()

def joinpath(rootdir, targetdir):
    return os.path.join(os.sep, rootdir + os.sep, targetdir)

# Path of EEA_data
path_main_dir_EEA_data = os.environ['EEA_data']

if path_main_dir_EEA_data == "":
    print("Error: set the environmental variables of EEA data")
    exit(-1)

# Main data directory 
DATADIR = joinpath(path_main_dir_EEA_data, "Raw_metainfo")

if not os.path.exists(DATADIR):
  os.mkdir(DATADIR)

# Client Airbase
client = airbase.AirbaseClient()

list_of_air_poll = ["CO", "NO2", "O3", "PM2.5", "PM10", "SO2"]
list_of_countries = client.all_countries

dict_air_poll_code = {}

dict_air_poll_code["CO"] = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/10"
dict_air_poll_code["NO2"] = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/8"
dict_air_poll_code["O3"] = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7"
dict_air_poll_code["PM2.5"] = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/6001"
dict_air_poll_code["PM10"] = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"
dict_air_poll_code["SO2"] = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1"

parser = argparse.ArgumentParser(description='Script for downloading EEA stations')
parser.add_argument('-c','--country', help='Country selected', choices=list_of_countries,\
                    required=True)
args = vars(parser.parse_args())

country = args["country"]

# Path Regioni
dir_path_italy_region = joinpath(DATADIR, "Reg01012023")
path_italy_region_shp = joinpath(dir_path_italy_region, "Reg01012023_WGS84.shp")

# Path Province
dir_path_italy_prov = joinpath(DATADIR, "ProvCM01012023")
path_italy_prov_shp = joinpath(dir_path_italy_prov, "ProvCM01012023_WGS84.shp")

# Path Comuni
dir_path_italy_comuni = joinpath(DATADIR, "Com01012023")
path_italy_comuni_shp = joinpath(dir_path_italy_comuni, "Com01012023_WGS84.shp")

# Path of metainfo file
raw_path_meta_info = joinpath(DATADIR, "All_metainfo.csv")

if not os.path.exists(raw_path_meta_info):
    client.download_metadata(raw_path_meta_info)

new_path_meta_info = joinpath(os.getcwd(), "New_metainfo")

if not os.path.exists(new_path_meta_info):
  os.mkdir(new_path_meta_info)

# Index 5: AirQualityStationEoICode
df_metainfo = pd.read_table(raw_path_meta_info, header=[0], index_col=5, low_memory=False)

# Stations filtering due to the county specified
df_metainfo_country = df_metainfo.loc[df_metainfo['Countrycode'] == country]

df_metainfo_country = df_metainfo_country.filter(items=[
                                                            'AirPollutantCode', 'Longitude', 'Latitude',
                                                            'Altitude', 'ObservationDateBegin', 'ObservationDateEnd',
                                                            'AirQualityStationType'
                                    ])

# Reset DataFrame with columns in desired order
df_metainfo_country = df_metainfo_country[[
                                            'AirPollutantCode', 'Longitude', 'Latitude',
                                            'Altitude', 'ObservationDateBegin', 'ObservationDateEnd',
                                            'AirQualityStationType'
                        ]]

# Add spatial information of EEA stations to Italy country
if country == "IT":
    df_metainfo_country.insert(loc=1, column='Regione', value="N/A")
    df_metainfo_country.insert(loc=1, column='Provincia', value="N/A")
    df_metainfo_country.insert(loc=1, column='Comune', value="N/A")

    # Read of Shapefiles (Regioni / Province / Comuni)
    shp_italy_region = gpd.read_file(path_italy_region_shp)
    shp_italy_prov = gpd.read_file(path_italy_prov_shp)
    shp_italy_comuni = gpd.read_file(path_italy_comuni_shp)

    # Change of reference system WGS84 --> EPSG4326
    # [Longitude, Latitude]
    shp_italy_region = shp_italy_region.to_crs('epsg:4326')
    shp_italy_prov = shp_italy_prov.to_crs('epsg:4326')
    shp_italy_comuni = shp_italy_comuni.to_crs('epsg:4326')

    for air_pol in list_of_air_poll:
        
        print("Current air pollutant: ", air_pol)

        # Filtering due to current air pollutant
        df_metainfo_country_air_pol = df_metainfo_country.loc[  
                                                        df_metainfo_country['AirPollutantCode'] == \
                                                        dict_air_poll_code[air_pol]
                                                    ]


        # For all stations that measure the current air pollutant
        for current_cod_station in df_metainfo_country_air_pol.index:
            
            print("Current code station EEA: ", current_cod_station)

            current_df_station = df_metainfo_country_air_pol.filter(like=current_cod_station, axis=0)

            lon_current_station = current_df_station["Longitude"][0]
            lat_current_station = current_df_station["Latitude"][0]

            point_current_station = Point(lon_current_station, lat_current_station)

            # ---------------- Regione ----------------
            for index, row in shp_italy_region.iterrows():
                poly_region = shp_italy_region.at[index, "geometry"]

                if poly_region.contains(point_current_station):
                    name_region = shp_italy_region.at[index, "DEN_REG"]
                    df_metainfo_country_air_pol.loc[current_df_station.index,"Regione"] = name_region
                    break

            # ---------------- Provincia ----------------
            for index, row in shp_italy_prov.iterrows():
                poly_prov = shp_italy_prov.at[index, "geometry"]

                if poly_prov.contains(point_current_station):
                    name_prov = shp_italy_prov.at[index, "DEN_PROV"]

                    # Vuol dire che è capoluogo
                    if name_prov == "-":
                        name_prov = shp_italy_prov.at[index, "DEN_CM"]

                    df_metainfo_country_air_pol.loc[current_df_station.index,"Provincia"] = name_prov
                    break

            # ---------------- Comune ----------------
            for index, row in shp_italy_comuni.iterrows():
                poly_comune = shp_italy_comuni.at[index, "geometry"]

                if poly_comune.contains(point_current_station):
                    name_comune = shp_italy_comuni.at[index, "COMUNE"]
                    df_metainfo_country_air_pol.loc[current_df_station.index,"Comune"] = name_comune
                    break
                
        path_metainfo_country_air_pol = joinpath(new_path_meta_info, air_pol + "_" + country + "_metainfo.csv")
        df_metainfo_country_air_pol.to_csv(path_metainfo_country_air_pol)

else:

    for air_pol in list_of_air_poll:

        # Filtering due to current air pollutant
        df_metainfo_country_air_pol = df_metainfo_country.loc[  
                                                        df_metainfo_country['AirPollutantCode'] == \
                                                        dict_air_poll_code[air_pol]
                                                    ]

        path_metainfo_country_air_pol = joinpath(new_path_meta_info, air_pol + "_" + country + "_metainfo.csv")
        df_metainfo_country_air_pol.to_csv(path_metainfo_country_air_pol)
