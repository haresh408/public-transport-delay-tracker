import os
from datetime import datetime
from dataset_helper_methods import create_dirs_and_get_path

import pandas as pd
import numpy as np



path_link = 'data/local_train_dataset.csv'

# Generating Date and Time
current_datetime = datetime.now()
current_date = current_datetime.date()


# Reading the CSV file
script_dir = os.path.dirname(os.path.abspath(__file__)) 
config_path = os.path.join(script_dir,path_link)

df = pd.read_csv(config_path,encoding='latin-1')
n = len(df)

#assigning unique station IDs
df['stationId'] = [str(i).zfill(4) for i in range(1,n +1)]

latitudes = np.linspace(-90, 90, n).astype(str)
longitudes = np.linspace(-180, 180, n).astype(str)
np.random.shuffle(latitudes)
np.random.shuffle(longitudes)

df['latitude'] = latitudes
df['longitude'] = longitudes

# Extracting relevant columns and converting to a list of lists
station_data = df[['stationId','Station','Station Code','latitude','longitude']]

# renaming columns to standardize column names
station_data = station_data.rename(columns={'Station': 'stationName', 'Station Code': 'stationCode'})


station_data.to_csv(os.path.join(create_dirs_and_get_path(),'station_info.csv'), index=False)

def get_station_code():
    return station_data['stationCode'].to_list()




