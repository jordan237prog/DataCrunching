import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt

def check_file(filename):
    """
    if the file in path exists return True, else False
    """
    fullname = os.path.join(os.getcwd(),filename)
    
    if os.path.isfile(fullname):
        return True
    else:
        return False
# we create this function to be resuable 


def read_dataset(path): 
    '''
    the function reads the dataset via a path and retunrs a dataframe 

    '''
    if check_file(path) == True:
        df=pd.read_csv(path)
    else:
        df = None
    return df


def list_files_cur():
    '''
    This function returns the list of the files in the current directory
    '''
    current_directory = os.getcwd()
    result = os.listdir(current_directory)
    return result

def list_file_with_extension_cur_glob(extension):
    '''
    This function returns the list of the files with a given extension in the current directory
    using glob
    '''
    extension = extension.strip()
    if extension.startswith('.'): # Check if there is a . at the begining of the extension
        extension = extension[1:] # if yes, select only the extension fomr the 2nd character
    allowed = ['csv','data','txt'] # list of allowed extensions
    try:
        if extension not in allowed:
            raise ValueError
        else:
            current_directory = os.getcwd()
            full_path = os.path.join(current_directory, '*.' + extension)
            return glob.glob(f"*.{extension}")
    except:
        print('Extension used is not allowed, please used one of these : ')
        print(str(allowed))

        
def join_city_to_ticket_data (cities, ticket_data):
    '''
    This function joins the cities dataframe to the  tikets data, respecting the oringin city and 
    destination city columns, it also adds new columns for the origin and destination latitude, longitude,
    city unique name, cities local names and city population
    '''
    #This block of code is used to just merge the cities data and the tiket_data dataframes in a consistent way
    #  cities_unique_name = cities.set_index('id')['unique_name'].to_dict()
    # cities_unique_name = dict(cities[['id', 'unique_name']].values)
    
    city_unique_name = cities.set_index('id')['unique_name']
    city_local_names = cities.set_index('id')['local_name']
    city_latitude = cities.set_index('id')['latitude']
    city_longitude = cities.set_index('id')['longitude']
    city_population = cities.set_index('id')['population']


    ticket_data['o_city_unique_name'] = ticket_data['o_city'].map(city_unique_name)
    ticket_data['d_city_unique_name'] = ticket_data['d_city'].map(city_unique_name)

    ticket_data['o_city_local_name'] = ticket_data['o_city'].map(city_local_names)
    ticket_data['d_city_local_name'] = ticket_data['d_city'].map(city_local_names)

    ticket_data['o_city_latitude'] = ticket_data['o_city'].map(city_latitude)
    ticket_data['o_city_longitude'] = ticket_data['o_city'].map(city_longitude)
    
    ticket_data['d_city_latitude'] = ticket_data['d_city'].map(city_latitude)
    ticket_data['d_city_longitude'] = ticket_data['d_city'].map(city_longitude)

    ticket_data['o_city_population'] = ticket_data['o_city'].map(city_population)
    ticket_data['d_city_population'] = ticket_data['d_city'].map(city_population)

    return ticket_data


def change_date_data_type_from_string_to_datetime(ticket_data):
    '''
    this function takes the ticket_data dataframe and changes the dates data type from string to datetime
    for data type consistency and to permit use to get the jouney duration easily
    '''
    ticket_data['departure_ts'] =  pd.to_datetime(ticket_data['departure_ts'])
    ticket_data['arrival_ts'] =  pd.to_datetime(ticket_data['arrival_ts'])
    ticket_data['search_ts'] =  pd.to_datetime(ticket_data['search_ts'])
    
    return ticket_data


def add_trip_duration_column_to_ticket_data(ticket_data):
    '''
    NOTE: must be executed after converting the dates data types to datetime
    takes the ticket_data frame as parameter and add a columnn duration in hours using the departure_ts 
    and the arrival_ts colombs of the ticket_data dataframe
    '''
    ticket_data['duration_in_hrs'] = (ticket_data['arrival_ts']-ticket_data['departure_ts']) / pd.Timedelta(hours=1)
    return ticket_data


def replace_nan_for_middle_stations_and_other_companies_with_zero(ticket_data):
    '''
    this functions takes ticket_data as a parameter, then replaces all the NaN in the middle_stations and
    other_companies with Zero.
    this indicates that there in no middle station for a particular trip and no other company does the trip
    '''
    ticket_data['middle_stations'] = ticket_data['middle_stations'].fillna(0) #no middle station
    ticket_data['other_companies'] = ticket_data['other_companies'].fillna(0) #no other company
    return ticket_data


def clean_up_providers_data_frame(providers):
    '''
    this function takes in the provider data frame and returns a clean version of it, it replaces NaN and 
    removes the provider_id column which is inconsistent and can be replaced with the company inoder to join
    with the tickets_data dataframe
    '''
    for i in ['has_wifi','has_plug','has_adjustable_seats','has_bicycle']:
        providers[i] = providers[i].replace( np.nan, False)
    clean_providers = providers.drop('provider_id', axis=1)
    return clean_providers


def merge_ticket_to_station(tickets, stations):
    '''this fuction takes the tickets and stations as parameters, then merges them to form one dataframe
    '''
    station_unique_name = stations.set_index('id')['unique_name']
    station_latitude = stations.set_index('id')['latitude']
    station_longitude = stations.set_index('id')['longitude']

    tickets['o_station_unique_name'] = tickets['o_station'].map(station_unique_name)
    tickets['d_station_unique_name'] = tickets['d_station'].map(station_unique_name)

    tickets['o_station_latitude'] = tickets['o_station'].map(station_latitude)
    tickets['o_station_longitude'] = tickets['o_station'].map(station_longitude)

    tickets['d_station_latitude'] = tickets['d_station'].map(station_latitude)
    tickets['d_station_longitude'] = tickets['d_station'].map(station_longitude)

    return tickets


def distance(data, d_type='city'):
    """
    Calculate the Haversine distance.
    Parameters
    ----------
    dataframe :  data['o_city_latitude']
                 data['o_city_longitude']
                 data['d_city_latitude']
                 data['d_city_longitude']
     distance type(d_type) : which is a string(either city or station)
    Returns
    -------
    a dataframe containing distance_in_km beteen the set of lat long : float
    """

    lat_1 = data['o_'+ d_type +'_latitude']
    lon_1 = data['o_'+ d_type +'_longitude']
    
    lat_2 = data['d_'+ d_type +'_latitude']
    lon_2 = data['d_'+ d_type +'_longitude']
    
    radius = 6371  # km

    dlat = np.radians(lat_2 - lat_1)
    dlon = np.radians(lon_2 - lon_1)

    a = (np.sin(dlat / 2) * np.sin(dlat / 2) +
         np.cos(np.radians(lat_1)) * np.cos(np.radians(lat_2)) *
         np.sin(dlon / 2) * np.sin(dlon / 2))
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    data[d_type + '_distance_km'] = radius * c

    return data

def price_stats_per_travel(data, place='city'):
    '''this function take a dataframe as parameter and returns a dataframe with min max and avarage price per       trip
    '''
    return (data.set_index(["o_" + place,"d_" + place])['price_in_cents']
           .groupby(level=[0,1])
           .agg(['min','max','mean']))


def duration_stats_per_travel(data, place='city'):
    '''this function take a dataframe as parameter and returns a dataframe with min max and avarage travel
    duration per trip
    '''
    return (data.set_index(["o_" + place,"d_" + place])['duration_in_hrs']
           .groupby(level=[0,1])
           .agg(['min','max','mean']))

def distance_range(all_data_trip_distance,quantile):
    require_table = all_data_trip_distance.loc[:, ['city_distance_km','duration_in_hrs','price_in_cents','transport_type']]
    if quantile == 1:
        condition = require_table['city_distance_km'] <= 200
        return require_table[condition]
    elif quantile == 2:
        condition=np.logical_and(require_table['city_distance_km']>200,require_table['city_distance_km']<=800)
        return require_table[condition]
    elif quantile == 3:
        condition=np.logical_and(require_table['city_distance_km']<=2000,require_table['city_distance_km']>800)
        return require_table[condition]
    elif quantile == 4:
        condition = require_table['city_distance_km']>=1880
        return require_table[condition]
    else:
        return ('invalide quantile should be in (1,2,3,4) or in valide dataset')

    
def get_barplot_of_transport_type_duration(dataframe):
    '''
    takes in a a dataframe and returns a bar chart
    '''
    
    condition_bus = dataframe['transport_type'] == 'bus'
    bus = dataframe[condition_bus]

    condition_train = dataframe['transport_type'] == 'train'
    train = dataframe[condition_train]

    condition_carpooling = dataframe['transport_type'] == 'carpooling'
    carpooling = dataframe[condition_carpooling]

    bus_duration = bus['duration_in_hrs'].mean()
    bus_price = bus['price_in_cents'].mean()

    train_duration = train['duration_in_hrs'].mean()
    train_price = train['price_in_cents'].mean()

    carpooling_duration = carpooling['duration_in_hrs'].mean()
    carpooling_price = carpooling['price_in_cents'].mean()

    plt.bar(['Bus','Train','Carpooling'], [bus_duration,train_duration, carpooling_duration], color ='maroon', width = 0.4)

    plt.xlabel("transport Duration")
    plt.ylabel("Duration")
    plt.title("Transport Type vs mean Duration")
    return plt.show()



def get_barplot_of_transport_type_price(dataframe):
    '''
    takes in a a dataframe and returns a bar chart
    '''
    
    condition_bus = dataframe['transport_type'] == 'bus'
    bus = dataframe[condition_bus]

    condition_train = dataframe['transport_type'] == 'train'
    train = dataframe[condition_train]

    condition_carpooling = dataframe['transport_type'] == 'carpooling'
    carpooling = dataframe[condition_carpooling]

    bus_duration = bus['duration_in_hrs'].mean()
    bus_price = bus['price_in_cents'].mean()

    train_duration = train['duration_in_hrs'].mean()
    train_price = train['price_in_cents'].mean()

    carpooling_duration = carpooling['duration_in_hrs'].mean()
    carpooling_price = carpooling['price_in_cents'].mean()

    plt.bar(['Bus','Train','Carpooling'], [bus_price,train_price, carpooling_price], color ='maroon', width = 0.4)

    plt.xlabel("transport Price")
    plt.ylabel("Price in cents")
    plt.title("Transport Type vs mean price")
    return plt.show()
