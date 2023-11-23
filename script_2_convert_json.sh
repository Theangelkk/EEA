#!/bin/bash
conda init bash
conda activate EEA

export EEA_data="/Volumes/LaCie/Air_Pollution_datasets/EEA"

# Declare a string array with type
declare -a list_air_pol=("PM2.5" "PM10" "CO" "NO2" "O3" "SO2")

start_year=2013
end_year=2023

# Read the array values with space
for air_pol in "${list_air_pol[@]}"; do

    python ./1_download_EEA_data.py -a $air_pol -c IT -freq_mode hour -start_year $start_year -end_year $end_year
    python ./1_download_EEA_data.py -a $air_pol -c IT -freq_mode day -start_year $start_year -end_year $end_year

    python ./2_convert_json.py -a $air_pol -c IT -freq_mode hour -start_year $start_year -end_year $end_year
    python ./2_convert_json.py -a $air_pol -c IT -freq_mode day -start_year $start_year -end_year $end_year

done