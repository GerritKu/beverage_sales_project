import pandas as pd
import logging
import datetime
from time import sleep
from geopy.geocoders import Nominatim
import numpy as np
from pathlib import Path

#Functions cleaning client data

##################################################################################
##################################################################################

def fill_nan_adress(df):
    
    """This function fills missing adresses with a default address. 

    Args:
        df (dataframe): dataframe of client data

    Returns:
        dataframe: with missing adress columns filled by default adress.
    """
    #check if mandatory columns are within the xml file / dataframe
    for col in ['PLZ','Ort','Str..1','Kunden-Nr.']:
        assert col in df.columns,  f'The necessary column {col} was not found'
    
    #check potential columns for NaN and...
    columns_check = ['Str..1','PLZ']
    for cols in columns_check:
        mask = df[cols].isnull()

        #...replace them with a deafault,remote location in Island
        df.loc[mask,'Str..1'] = "Norðausturvegur"
        df.loc[mask,'PLZ'] = 78585
        df.loc[mask,"Ort"] = 'Island'
        #keep a log of replaced values
        for client in df[mask]['Kunden-Nr.']:
            logging.info(f'Defaulted the adress of client {client} due to missing values')

    return df

##################################################################################
##################################################################################

def clean_client(df):
    """This function returns a cleaned employee data frame. Its necessity within the random sample data is 
    not given and only serves for presentation purposes to understand the structure of the original code.

    Args:
        df (dataframe): dataframe of client data

    Returns:
        dataframe: with missing adress columns filled by default adress.
    """
    #check if mandatory columns are within the xml file / dataframe
    for col in ['Erstanlage am','Str..1','Kunden-Nr.','Kunde ab','Name 1',
                'Ort','PLZ','Preisgrp.']:
        assert col in df.columns,  f'The necessary column {col} was not found'

    #fill NaN adresses
    df_new = fill_nan_adress(df)
    
    #check if all future NaNs are cleaned
    if df_new[['Erstanlage am','Str..1','Kunden-Nr.','Name 1','Ort','PLZ']].isnull().sum().sum() != 0:
          logging.error(''''Error: There are still missing values in kundenstammdaten after cleaning''')

    return df_new[['Kunden-Nr.','Name 1','Erstanlage am',
                    'Str..1','Ort','PLZ',
                    'suburb','latitude','longitude']]

#Function to add geo-data

##################################################################################
##################################################################################

def get_geo(df,update_all=False):
    """This function gets additional geo data based on Str..1,'PLZ code and Ort data using geopy. 
    In the case of the sample data its only loading pre-enriched data.

    Args:
        df (_dataframe_): dataframe of client data, including Str..1,'PLZ code and Ort
        update_all (bool, optional): Set to true in order to enrich the location data by latitude, longitude, 
        country, suburb, borough and country_code.

    Returns:
        _dataframe_: dataframe with enriched locational data and columns for latitude, longitude, 
        country, suburb, borough and country_code
    """
    #check if mandatory columns are within the xml file / dataframe
    for col in ['Str..1','Kunden-Nr.','Ort','PLZ']:
        assert col in df.columns,  f'The necessary column {col} was not found'

    #Since runtime of geo data extraction is high (~6min) sampledata is provided and loaded instead
    if update_all:

        #get adresses per client as string
        df_adresse = df[['Str..1','PLZ','Ort','Kunden-Nr.']].copy()
        df_adresse.drop_duplicates(inplace=True)
        dict_adress = {}
        for idx,value in df_adresse.iterrows():
            dict_adress[value['Kunden-Nr.']] = (str(value["Str..1"]) + " " + str(value['PLZ']) + " " + str(value["Ort"]))
        #use geopy & open street map to get data on adresse
        geolocator = Nominatim(user_agent='hallo@test.berlin')

        geo_dict_lati = {}
        geo_dict_longi = {}
        geo_dict_raw = {}
        dict_exceptions = {}
        for key in dict_adress.keys():
            try:
                location = geolocator.geocode(dict_adress[key],addressdetails=True)
                geo_dict_lati[key] = location.latitude
                geo_dict_longi[key] = location.longitude
                try:
                    geo_dict_raw[key] = location.raw['address']
                except:
                    logging.warning(f"No raw data for {key}")
                    dict_exceptions[key] = (dict_adress[key])
            except:
                logging.warning(f"No long/lat for {key}")
                dict_exceptions[key] = (dict_adress[key])
                sleep(1)
            sleep(1)
        
        #Extract additional information from raw data
        country_dict = {}
        suburb_dict = {}
        borough_dict = {}
        country_code_dict = {}
        for key in geo_dict_raw.keys():
            try:
                country_dict[key] = geo_dict_raw[key]['country']
            except:
                country_dict[key] = np.nan
            try:
                suburb_dict[key] = geo_dict_raw[key]['suburb']
            except:
                suburb_dict[key] = np.nan
            try:
                borough_dict[key] = geo_dict_raw[key]['borough']
            except:
                borough_dict[key] = np.nan
            try:
                country_code_dict[key] = geo_dict_raw[key]['country_code']
            except:
                country_code_dict[key] = np.nan
        
        #combine to dataframe:
        geo_dict_dict = {'longitude' : geo_dict_longi,
                    'country' : country_dict,
                    'suburb' : suburb_dict,
                    'borough' : borough_dict,
                    'country_code' : country_code_dict
                    }

        df_geo = pd.DataFrame(geo_dict_lati, index = ['latitude']).T
        for index,dict in geo_dict_dict.items():
            df_temp = pd.DataFrame(dict, index = [index]).T  
            df_geo = pd.merge(df_geo,df_temp,how="left",left_index=True, right_index=True)
        
        df_final = df_geo

    else:
        PATH = Path(__file__).parent/'data'
        df_final = pd.read_csv((Path(f'{PATH}/random_geo.csv')),index_col=False)

    return df_final

##################################################################################
##################################################################################

#Functions for clean absatzdaten

def clean_sales(df):
    """
    This function returns a cleaned sales dataframe by dropping unnecessary columns, checking for missing values
     and renaming unspecific columns. Its necessity within the random sample data is limited and 
     only serves for presentation purposes to understand the structure of the original code.
    Args:
        df (dataframe): dataframe of beverage sales data within 2020-2022

    Returns:
        dataframe: cleaned sales dataframe
    """
    #drop Nan within Kunden-Nr.
    df.drop(df[df['Kunden-Nr.'].isnull()].index, inplace=True)
    for index in df[df['Kunden-Nr.'].isnull()].index:
        logging.info(f'Deleted row {index} due to missing Kunden-Nr.')


    #rename columns
    df.rename(columns={'Ebene 1' : "ebene_1", 
               'Bezeichnung' : "ebene_1_name", 
               'Ebene 2' : "ebene_2", 
               'Bezeichnung.1': "ebene_2_name", 
               'Ebene 3':"ebene_3",
                'Bezeichnung.2':"ebene_3_name", 
                'Wgr.  Bez.': 'Wgr_Bez.',
                'Bezeichnung.3': "umsatzart_name",
                'Bezeichnung.4': 'vorg.art_name', 
                'Bezeichnung.5': 'vers.art_name',
                'Ebene 4':"ebene_4",
                'Bezeichnung.6':"ebene_4_name",
                'Stat.Menge': 'Flaschen', 
                'VK-Menge Ka': 'Kisten',
                'Lag' : "Lager"
                },inplace=True)

    #Fill NaN based on kundencode values
    list_kundencode = ['ebene_1','ebene_2','ebene_3','ebene_4']
    list_kundencode_name = ['ebene_1_name','ebene_2_name','ebene_3_name','ebene_4_name']
    for col1,col2 in zip(list_kundencode,list_kundencode_name):
        df.loc[df[col1] == 0,col2] = 0
    
    #check if all future NaNs are cleaned
    if df.isnull().sum().sum() != 0:
        logging.error('''Error: There are still missing values in sales after cleaning''')

    return df

##################################################################################
##################################################################################

#Functions cleaning employee data

def get_clean_employee(df_mitarbeitende):
    
    """This function replaces missing values and checks for existance of mandatory columns.

    Returns:
        dataframe: cleaned dataframe of employee to client connection
    """
    
    #Info for future user to be careful when using updated files with different # of columns
    if df_mitarbeitende.shape[1] != 7:
        logging.warning('''Your excel sheet has a different number of columns than the initial 
                        file with 7 columns.''')

    #check if mandatory columns are within the xml file / dataframe
    assert 'Kunden-Nr.' in df_mitarbeitende.columns, 'The necessary column Kunden_Nr. was not found'
    assert 'Vtr.Nr.' in df_mitarbeitende.columns, 'The necessary column Vtr.Nr. was not found'
    assert 'Erstvertreter' in df_mitarbeitende.columns, 'The necessary column Erstvertreter was not found'

    #Keep employee to Customer mapping unique by only keeping "Erstvertreter"
    df_mitarbeitende.sort_values(by='Erstvertreter',ascending=False,inplace=True)
    df_mitarbeitende = df_mitarbeitende.drop_duplicates(subset=['Kunden-Nr.'],keep='first')

    #fill NaN 'Vtr.Nr.' with one artificial employee #
    df_mitarbeitende.loc[df_mitarbeitende['Vtr.Nr.'].isnull(),'Vtr.Nr.'] = 999

    #check if all future NaNs are cleaned
    if df_mitarbeitende.isnull().sum().sum() != 0:
        logging.error('''Error: There are still missing values after clean merging Vtr.Nr. and Kunden-Nr.''')
    
    return df_mitarbeitende
    

