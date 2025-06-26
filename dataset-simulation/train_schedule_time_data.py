#import section
import os
import random
import uuid
from datetime import timedelta, datetime

import pandas as pd

from train_master_data import get_train_number, get_df_length
from station_info_data import get_station_code
from dataset_helper_methods import create_dirs_and_get_path


train_master_data_path = os.path.join(create_dirs_and_get_path(), 'train_master_data.csv')
train_master_data = pd.read_csv(train_master_data_path,encoding='latin-1')

data_path_link = 'data/trains_dataset.csv'
master_data = pd.read_csv(data_path_link,encoding='latin-1')


#variables
trainIds = train_master_data['trainId'].tolist()
train_no_and_names = list(zip(master_data['Train no.'].tolist(), master_data['Train name'].tolist()))
trainNames = master_data['Train no.'].to_list()
trainNumbers = master_data['Train name'].to_list()


n = get_df_length()

# Getting random 20 station codes from the stationInfoData
def pick_20_station_codes():
    return random.sample(get_station_code(), random.choice([20,30,40,50]))


# Random arrival time in hh:mm format
# Random departure time in arrival time + 5 or 10 minutes in hh:mm format
def get_arrival_and_departure_time():
    arrival_times = []
    departure_times = []
    for _ in range(n):
    # Generate random times for arrival and departure
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        arrival_times.append(f"{hour:02}:{minute:02}")
        departure_times.append(timedelta(minutes = random.choice([5,10])))

    return {"arrival_times":arrival_times, "departure_times":departure_times}

arrival_time = get_arrival_and_departure_time().get("arrival_times")
departure_time = get_arrival_and_departure_time().get("departure_times")


# Generating Date and Time
current_datetime = datetime.now()
current_date = current_datetime.date()



# dataframe schema and data generation
data = {
    "scheduleId": [str(uuid.uuid4()) for _ in range(n)],
    "trainId": trainIds,
    "trainNumber": trainNumbers,
    "trainName": trainNames,
    "scheduledArrivalTime": arrival_time ,
    "scheduledDepartureTime": departure_time,
    "stationCodes": [pick_20_station_codes() for _ in range(n) ],
    "scheduleDate":[str(current_date) for _ in range(n)],
    "createdDate": [str(current_datetime) for _ in range(n)]
}




# Creating the DataFrame
df = pd.DataFrame(data)


# Saving the DataFrame to a CSV file
df.to_csv(os.path.join(create_dirs_and_get_path(),'train_schedule_time_data.csv') , index=False)


def get_train_id_and_station_codes():
    return list(zip(df['trainId'].tolist(), df['stationCodes'].tolist()))