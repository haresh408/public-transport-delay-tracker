import datetime
import json
import os.path
import random
import uuid
import pandas as pd
import ast

from dataset_helper_methods import create_dirs_and_get_path



path_link = os.path.join(create_dirs_and_get_path(),'train_schedule_time_data.csv')
train_schedule_data = pd.read_csv(path_link,encoding='latin-1')

trainIds = train_schedule_data['trainId'].unique().tolist()
station_codes = list(zip(train_schedule_data['trainId'].tolist(), train_schedule_data['stationCodes'].tolist()))




with open('data_model.json', 'r') as f:
    data = json.load(f)
    data = data['trainLiveStatus']


# Helper Methods

def random_distance_picker():
    distance = random.randint(50,2000)
    return distance

def random_time_hhmm():
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    return f"{hour:02d}:{minute:02d}"

def minutes_difference(time1, time2):
    fmt = "%H:%M"
    t1 = datetime.datetime.strptime(time1, fmt)
    t2 = datetime.datetime.strptime(time2, fmt)
    diff = t2 - t1
    return int(diff.total_seconds() // 60)

def adding_delay_to_actual_departure_time(expected_arrival_time):
    time = datetime.datetime.strptime(expected_arrival_time, "%H:%M")
    delay_options = random.randint(5,50)
    time += datetime.timedelta(minutes = delay_options)
    time_str = str(time.strftime("%H:%M"))
    return random.choice([time_str,expected_arrival_time])

def get_station_code(train_id):

    station_code_dict = dict(station_codes)
    row = ast.literal_eval(station_code_dict.get(train_id))
    if row:
        return row[0]
    else:
        return str()

def get_next_station_code(train_id, station_code,event_type):
    station_code_dict = dict(station_codes)
    if event_type == 'Arrival':
        row_data = ast.literal_eval(station_code_dict.get(train_id))
        if row_data:
            for i in range(len(row_data)):
                if row_data[i] == station_code:
                    return row_data[i + 1]
    else:
        return station_code

def get_event_type(event_type):
    if event_type == 'Arrival':
        return 'Departure'
    elif event_type == 'Departure':
        return 'Arrival'
    else:
        return random.choice(['Arrival', 'Departure'])

def get_distance_covered(distance_covered, total_distance, station_code, train_id):
    station_code_data = dict(station_codes)
    station_code_list_by_train_id = ast.literal_eval(station_code_data.get(train_id))

    index = station_code_list_by_train_id.index(station_code)
    remaining_stations = len(station_code_list_by_train_id) - index - 1

    if distance_covered == '':
        current = 0
    else:
        current = int(distance_covered)

    remaining_distance = int(total_distance) - current

    if remaining_stations <= 0 or remaining_distance <= 0:
        return str(current)

    # Distribute distance
    avg_step = remaining_distance // remaining_stations
    step = random.randint(max(1, avg_step - 2), avg_step + 2)  # small variation
    new_distance = min(current + step, int(total_distance))

    return str(new_distance)

def get_expected_arrival_time(expected_departure_time):
    if expected_departure_time == '' or expected_departure_time.lower() == 'nan':
        return random_time_hhmm()
    else:
        time = datetime.datetime.strptime(expected_departure_time, "%H:%M")
        delay_options = random.randint(55, 180)
        time += datetime.timedelta(minutes=delay_options)
        return str(time.strftime("%H:%M"))

def get_actual_arrival_time(actual_departure_time, expected_arrival_time):
    if actual_departure_time == '' or actual_departure_time.lower() == 'nan':
        return str(random_time_hhmm())
    elif expected_arrival_time == '' or expected_arrival_time.lower() == 'nan':
        return str(random_time_hhmm())
    else:
        ad_time = datetime.datetime.strptime(actual_departure_time, "%H:%M")
        ea_time = datetime.datetime.strptime(expected_arrival_time, "%H:%M")
        seconds_diff = minutes_difference(str(ad_time.strftime("%H:%M")), str(ea_time.strftime("%H:%M")))
        delay_options = random.randint(1, 30)
        ad_time += datetime.timedelta(minutes=(seconds_diff + delay_options))
        return str(ad_time.strftime("%H:%M"))

def get_expected_departure_time(expected_arrival_time):
    if expected_arrival_time == '' or expected_arrival_time.lower() == 'nan':
        return random_time_hhmm()
    else:
        time = datetime.datetime.strptime(expected_arrival_time, "%H:%M")
        time += datetime.timedelta(minutes = random.randint(5, 10))
        return str(time.strftime("%H:%M"))

def get_actual_departure_time(actual_arrival_time):
    if actual_arrival_time == '' or actual_arrival_time.lower() == 'nan':
        return random_time_hhmm()
    else:
        time = datetime.datetime.strptime(actual_arrival_time, "%H:%M")
        delay_options = random.randint(5, 20)
        time += datetime.timedelta(minutes=delay_options)
        return str(time.strftime("%H:%M"))

def check_if_last_station_code(train_id,station_code):
        station_code_data = dict(station_codes)
        row_data = ast.literal_eval(station_code_data.get(train_id))

        if station_code == row_data[-1]:
            return True
        else:
            return False



# Data Creation Methods

def create_batch_data(batch_size = 500):

    train_ids = random.sample(trainIds, k=500)
    batch_data = []

    for i in range(batch_size):
        new_data = dict()
        new_data["eventId"] = str(uuid.uuid4())
        new_data['trainId'] = train_ids[i]
        new_data['eventType'] = 'Departure'
        new_data['stationCode'] = get_station_code(new_data['trainId'])
        new_data['distanceCovered'] = '0'
        new_data['totalDistance'] = random_distance_picker()
        new_data['noOfDays'] = random.randint(1, 5)
        new_data['expectedArrivalTime'] = ''
        new_data['actualArrivalTime'] = ''
        new_data['expectedDepartureTime'] = random_time_hhmm()
        new_data['actualDepartureTime'] = adding_delay_to_actual_departure_time(new_data['expectedDepartureTime'])
        new_data['createdDate'] = str(datetime.datetime.now())

        batch_data.append(new_data)

    return batch_data

def train_live_status(batch_size=500):

    train_ids = random.sample(trainIds, k=500)
    data_path = os.path.join(create_dirs_and_get_path(False),'train_live_status_data.csv')

    if os.path.exists(data_path):
        df = pd.read_csv(data_path, encoding='latin-1')
        batch_data = []

        for i in range(1000):

            if len(batch_data) >= batch_size:
                break

            train_id = train_ids[len(batch_data) -1]
            train_id_record = df[df['trainId'] == train_id]

            if not train_id_record.empty:

                train_id_record.loc[:,'createdDate'] = pd.to_datetime(train_id_record['createdDate'])
                latest_record = train_id_record.sort_values(by='createdDate', ascending=False).iloc[0].to_dict()

                if check_if_last_station_code(train_id, latest_record['stationCode']):
                    continue

                latest_record['eventType'] = get_event_type(latest_record['eventType'])
                latest_record['stationCode'] = get_next_station_code(train_id,latest_record['stationCode'],latest_record['eventType'])
                latest_record['distanceCovered'] = get_distance_covered(latest_record['distanceCovered'], latest_record['totalDistance'],latest_record['stationCode'],latest_record['trainId'])
                latest_record['expectedArrivalTime'] = get_expected_arrival_time(str(latest_record['expectedDepartureTime']))
                latest_record['actualArrivalTime'] = get_actual_arrival_time(str(latest_record['actualDepartureTime']), str(latest_record['expectedArrivalTime']))
                latest_record['expectedDepartureTime'] = get_expected_departure_time(str(latest_record['expectedArrivalTime']))
                latest_record['actualDepartureTime'] = get_actual_departure_time(str(latest_record['actualArrivalTime']))
                latest_record['createdDate'] = str(datetime.datetime.now())

                batch_data.append(latest_record)

            else:
                new_data = dict()
                new_data["eventId"] = str(uuid.uuid4())
                new_data['trainId'] = train_id
                new_data['eventType'] = 'Departure'
                new_data['stationCode'] = get_station_code(train_id)
                new_data['distanceCovered'] = '0'
                new_data['totalDistance'] = random_distance_picker()
                new_data['noOfDays'] = random.randint(1, 5)
                new_data['actualArrivalTime'] = ''
                new_data['expectedArrivalTime'] = ''
                new_data['expectedDepartureTime'] = random_time_hhmm()
                new_data['actualDepartureTime'] = adding_delay_to_actual_departure_time(new_data['expectedDepartureTime'])
                new_data['createdDate'] = str(datetime.datetime.now())

                batch_data.append(new_data)


        return batch_data, True

    else:
        return create_batch_data(), False



#Data Generation and Saving

final_data = train_live_status()

final_df = pd.DataFrame(final_data[0])

if final_data[1]:
    final_df.to_csv(os.path.join(create_dirs_and_get_path(True),'train_live_status_data.csv'), mode='a',header=False,index=False)
else:
    final_df.to_csv(os.path.join(create_dirs_and_get_path(True), 'train_live_status_data.csv'), index=False)










