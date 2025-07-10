import os
import random
from datetime import datetime

departureStation = []
arrivalStation = []

current_datetime = datetime.now()
current_date = current_datetime.date()


#=======================================================================================

# returns a list of departure stations
def get_departure_station():
    return departureStation

# returns a list of arrival stations
def get_arrival_station():
    return arrivalStation

# returns a random average travel time between 1 and 500 minutes
def get_average_travel_time():
    average_time = random_floatnumber_upto6decimal(1,500)
    return average_time

# returns a random integer between 0 and 500
def get_no_of_cancelled_trains():
    return random.randint(0, 500)

# returns a random integer between 0 and 100
def get_no_of_late_trains_departure():
    return random.randint(1, 10)

def get_average_delay_late_trains_departure():
    late_average_delay = random_floatnumber_upto6decimal(1,50)
    return late_average_delay

# returns a random integer between 0 and 50 with any decimal places
def get_average_delay_all_trains_departure():
    all_average_delay = random_floatnumber_upto6decimal(1,50)
    return all_average_delay

# returns a random integer between 1 and 90
def get_no_of_late_trains_arrival():
    return random.randint(1, 90)

def get_average_delay_late_trains_arrival():
    late_average_delay = random_floatnumber_upto6decimal(1,100)
    return late_average_delay

def create_dirs_and_get_path(makedirs=True):
    PROJECT_ROOT = os.path.abspath(__file__)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'simulated-data', str(current_date))
    if makedirs:
        os.makedirs(path, exist_ok=True)
    return path


#========================================================================================

# returns a random integer between 1 and 6
def get_random_int1to6():
    return random.randint(0, 9)

# returns a random float number between lower_limit and upper_limit upto 6 decimal places
def random_floatnumber_upto6decimal(lower_limit,upper_limit,decimals_count = get_random_int1to6() ,count = 20):
    return [round(random.uniform(lower_limit, upper_limit), decimals_count) for _ in range (count)]

