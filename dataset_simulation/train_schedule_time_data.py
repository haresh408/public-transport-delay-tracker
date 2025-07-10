#import section
import os
import random
import uuid
from datetime import timedelta, datetime

import pandas as pd

from train_master_data import get_train_number, get_df_length
from station_info_data import get_station_code
from dataset_helper_methods import create_dirs_and_get_path


train_master_data_path = os.path.join(create_dirs_and_get_path(makedirs=False), 'train_master_data.csv')
train_master_data = pd.read_csv(train_master_data_path,encoding='latin-1')


# Reading the CSV file for train data
data_path_link = 'data/trains_dataset.csv'
script_dir = os.path.dirname(os.path.abspath(__file__)) 
config_path = os.path.join(script_dir,data_path_link )

master_data = pd.read_csv(config_path,encoding='latin-1')


#variables
trainIds = train_master_data['trainId'].tolist()
train_no_and_names = list(zip(master_data['Train no.'].tolist(), master_data['Train name'].tolist()))
trainNames = master_data['Train name'].to_list()
trainNumbers = master_data['Train no.'].to_list()


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

        #arrival time in hh:mm format
        arrival_time = f"{hour:02}:{minute:02}"
        
        #departure time is 5 or 10 minutes after arrival time
        time_obj = datetime.strptime(arrival_time, "%H:%M")
        delta = timedelta(minutes=random.choice([5, 10]))
        new_time = time_obj + delta
        departure_time = new_time.strftime("%H:%M")

    
        arrival_times.append(arrival_time)
        departure_times.append(departure_time)

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