import os
import random
import pandas as pd
from datetime import datetime
from dataset_helper_methods import create_dirs_and_get_path

operator_list = ['Govt', 'Private', 'Joint']
category_list = ['Express', 'Passenger', 'Superfast', 'Mail', 'Local']

# Generating Date and Time
current_datetime = datetime.now()
current_date = current_datetime.date()

data_path_link = 'data/trains_dataset.csv'



# Reading the CSV file
df = pd.read_csv(data_path_link,encoding='latin-1')
n = len(df)

# Data Generation
df['trainId'] = random.sample(range(1000000, 10000000), n)
df['operator'] = random.choices(operator_list, k=n)
df['category'] = random.choices(category_list, k=n)



train_master_df = df[['trainId','Train no.','category','operator']]

# Renaming Columns
train_master_df = train_master_df.rename(columns={'Train no.': 'trainNo'})


# Saving DataFrame to a CSV file
train_master_df.to_csv( os.path.join(create_dirs_and_get_path(),'train_master_data.csv'), index=False)



# Helpers Methods
def get_unique_train_ids():
    return train_master_df['trainId'].to_list()

def get_train_names():
    print(df.columns)
    new_df = df.rename(columns={'Train name': 'trainName'})
    return new_df['trainName'].to_list()

def get_train_number():
    return train_master_df['trainNo'].to_list()

def get_df_length():
    return n
