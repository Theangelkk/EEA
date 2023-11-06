# Librerie necessarie
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

def joinpath(rootdir, targetdir):
    return os.path.join(os.sep, rootdir + os.sep, targetdir)

# Creazione del dizionario delle regioni italiane
def empty_dict_italy_region():

    list_italy_region = [   "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", \
                        "Friuli Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", \
                        "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", \
                        "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"]
    dict_italy_region = {}

    for region in list_italy_region:
        
        dict_current_region = {}
        dict_current_region["N_stations"] = 0
        dict_current_region["stations"] = {}

        # Numero totale delle misurazioni
        dict_current_region["N_tot_measures"] = 0

        # Numero totale delle misurazioni valide
        dict_current_region["N_tot_valid_measures"] = 0

        # Numero totale dei valori mancanti
        dict_current_region["N_tot_missing_values"] = 0

        # Lista ordinata stazioni con minori tratti di missing values
        # e di minor lunghezza media
        dict_current_region["sorted_station_MDTs_min"] = []

        dict_italy_region[region] = dict_current_region

    return dict_italy_region

def fill_dict_station(  
                        Number_of_MDTs, Max_len_of_MDTs, Min_len_of_MDTs,
                        Avg_len_of_MDTs, N_tot_measures, N_tot_valid_measures,
                        N_tot_missing_values
                    ):

    dict_info_station = {}

    # Numero dei tratti di valori mancanti
    dict_info_station["Number_of_MDTs"] = Number_of_MDTs

    # Lunghezza massima di un tratto di valori mancanti
    dict_info_station["Max_len_of_MDTs"] = Max_len_of_MDTs

    # Lunghezza minima di un tratto di valori mancanti
    dict_info_station["Min_len_of_MDTs"] = Min_len_of_MDTs

    # Lunghezza media di un tratto di valori mancanti
    dict_info_station["Avg_len_of_MDTs"] = Avg_len_of_MDTs

    # Numero totale delle misurazioni
    dict_info_station["N_tot_measures"] = N_tot_measures

    # Numero totale delle misurazioni valide
    dict_info_station["N_tot_valid_measures"] = N_tot_valid_measures

    # Numero totale dei valori mancanti
    dict_info_station["N_tot_missing_values"] = N_tot_missing_values

    return dict_info_station

# Per accedere al servizio airbase, è necessario creare prima un aribase client
client = airbase.AirbaseClient()

list_air_pollutant = ["CO", "NO2", "O3", "PM2.5", "PM10", "SO2"]
list_countries = client.all_countries

parser = argparse.ArgumentParser(description='Metadata download of EEA')
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

# Path relativo ai metainfo files
path_metainfo_file = joinpath(os.getcwd(), "new_metainfo")
path_metainfo_file = joinpath(path_metainfo_file, air_poll_selected + "_" + country + ".csv")

# Path relativo ai dati memorizzati in formato CSV
path_dir_csv = joinpath(os.getcwd(), "csv_air_pol")
path_dir_air_pol_csv = joinpath(path_dir_csv, air_poll_selected)
path_dir_air_pol_csv = joinpath(path_dir_air_pol_csv, freq_mode)

# Ricerca del file CSV dei dati
list_files_dir_air_pol_csv = os.listdir(path_dir_air_pol_csv)
path_file_air_pol_data_csv = None

for file in list_files_dir_air_pol_csv:
    if file.endswith('.csv'):
        path_file_air_pol_data_csv = joinpath(path_dir_air_pol_csv, file)
        break

# Lettura del file di dati dell'inquinante richiesto
df_air_pol_data = pd.read_table(    path_file_air_pol_data_csv, delimiter=',', 
                                    header=[0], index_col=0, low_memory=False
                                )

if end_year <= start_year:
    print("Error: Start year must be greater then end year")
    exit(-1)

# Lettura del file di metainfo dell'inquinante della nazione definito
df_air_pol_metainfo = pd.read_table(path_metainfo_file, delimiter=',', index_col=0)

current_year = start_year

# Si analizzano il range temporale definito
for idx_year in range(start_year,end_year):

    print("Current year: ", str(current_year))

    start_date_current_year = datetime(current_year, 1, 1, 0, 0)

    if freq_mode == "hour":
        delta = 1
    else:
        delta = 24
    
    # Lettura file di testo
    with open(joinpath(path_dir_air_pol_csv, str(current_year) + ".txt")) as f:
        lines_current_year = f.readlines()

    if country == "IT":
        dict_country_current_year = empty_dict_italy_region()
    else:
        print("Implement for the country specified")
        exit(-1)

    # Andiamo a scorrere tutte le stazioni
    for cod_station in lines_current_year:

        # Se la linea che stiamo osservando è effettivamente
        # una stazione
        if country in cod_station:
            
            cod_station = cod_station.replace("\n","")

            print("Current code station: ", cod_station)

            # Andiamo a recuperare le info della stazione corrente all'interno
            # del file di metainfo
            df_current_station = df_air_pol_metainfo.filter(like=cod_station, axis=0)

            current_station_region = df_current_station["Regione"].values[0]

            dict_country_current_year[current_station_region]["N_stations"] += 1
        
            # --------------------- Calcolo Missing values --------------------- 
            # Andiamo a prendere tutte le rilevazioni di una specifica stazione
            df_current_station = df_air_pol_data.filter(like=cod_station, axis=0)
            
            # Ordinamento del DataFrame secondo la data di acquisizione
            df_current_station = df_current_station.sort_values(by='DatetimeBegin')

            # Numero delle misurazioni totali
            count_total_measures_current_station_current_year = 0

            # Numero delle misurazioni valide
            count_valid_measures_current_station_current_year = 0

            # Numeri dei missing values
            count_nan_measures_current_station_current_year = 0

            # Lunghezza massima di un tratto di missing values
            len_max_missing_values_current_station_current_year = 0

            # Lunghezza minima di un tratto di missing values
            len_min_missing_values_current_station_current_year = np.inf

            # Lunghezza media di un tratto di missing values
            len_avg_missing_values_current_station_current_year = 0

            # Numero di tratti di valori mancanti continui
            count_missing_data_tracts_current_station_current_year = 0

            # Se siamo in un tratto di valori mancanti
            current_MDT = False
            current_len_MDT = 0
            
            first_day = True

            for idx, row in df_current_station.iterrows():
                
                if str(current_year) in row["DatetimeBegin"]:

                    if first_day:
                        first_date_current_station = \
                                datetime.strptime(row["DatetimeBegin"].replace(' +01:00', ''), '%Y-%m-%d %H:%M:%S')

                        diff_dates = first_date_current_station - start_date_current_year
                        diff_dates = int(diff_dates.total_seconds() / (60*60*delta))

                        if diff_dates > 0:
                            count_missing_data_tracts_current_station_current_year += 1
                            len_max_missing_values_current_station_current_year = diff_dates
                            len_min_missing_values_current_station_current_year = diff_dates
                            len_avg_missing_values_current_station_current_year = diff_dates
                        
                        first_day = False

                    count_total_measures_current_station_current_year += 1

                    # Se siamo in presenza di un valore mancante
                    if math.isnan(row["Concentration"]):
                        
                        count_nan_measures_current_station_current_year += 1
                        current_len_MDT += 1

                        # Se è il primo valore di un tratto di missing values
                        if current_MDT == False:

                            count_missing_data_tracts_current_station_current_year += 1
                            current_MDT = True

                    # Se siamo in presenza di un valore numerico valido    
                    else:

                        count_valid_measures_current_station_current_year += 1

                        # Se è il primo valore dopo un tratto di missing values
                        if current_MDT == True:

                            len_avg_missing_values_current_station_current_year += current_len_MDT

                            # Aggiornamento se il nuovo tratto di missing values è di maggiore
                            # lunghezza
                            if len_max_missing_values_current_station_current_year < current_len_MDT:
                                len_max_missing_values_current_station_current_year = current_len_MDT

                            if len_min_missing_values_current_station_current_year > current_len_MDT:
                                len_min_missing_values_current_station_current_year = current_len_MDT

                            current_len_MDT = 0
                            current_MDT = False

            # Calcolo del valore medio della lunghezza dei tratti di valori mancanti
            if count_missing_data_tracts_current_station_current_year > 0:
                len_avg_missing_values_current_station_current_year /= float(count_missing_data_tracts_current_station_current_year)
            
            dict_country_current_year[current_station_region]["N_tot_measures"] += count_total_measures_current_station_current_year
            dict_country_current_year[current_station_region]["N_tot_valid_measures"] += count_valid_measures_current_station_current_year
            dict_country_current_year[current_station_region]["N_tot_missing_values"] += count_nan_measures_current_station_current_year

            dict_info_current_station = fill_dict_station(  
                                                            count_missing_data_tracts_current_station_current_year, 
                                                            len_max_missing_values_current_station_current_year, 
                                                            len_min_missing_values_current_station_current_year,
                                                            len_avg_missing_values_current_station_current_year, 
                                                            count_total_measures_current_station_current_year, 
                                                            count_valid_measures_current_station_current_year,
                                                            count_nan_measures_current_station_current_year
                                                    )
            
            dict_country_current_year[current_station_region]["stations"][cod_station] = dict_info_current_station

            # Ordinamento dizionario per lunghezza media dei valori mancanti minore
            dict_tmp = dict_country_current_year[current_station_region]["stations"].copy()
            #list_sorted = sorted(dict_tmp.items(), key=lambda t: (t[1]["Number_of_MDTs"], t[1]["Avg_len_of_MDTs"]))
            list_sorted = sorted(dict_tmp.items(), key=lambda t: t[1]["Max_len_of_MDTs"])

            dict_country_current_year[current_station_region]["sorted_station_MDTs_min"] = []
            for k in range(len(list_sorted)):
                dict_country_current_year[current_station_region]["sorted_station_MDTs_min"].append(list_sorted[k][0])

    # Salvataggio del dizionario in formato JSON
    path_current_year_json = joinpath(path_dir_air_pol_csv, str(current_year) + ".json")
    
    with open(path_current_year_json, 'w') as json_file:
        json_dumps_str = json.dumps(dict_country_current_year, indent=4)
        json_file.write(json_dumps_str)
    
    current_year += 1