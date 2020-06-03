#!/usr/bin/python3.6
'''
  Update databases that created by the create program initially.
  The station information within the domain will be retrieved.
  The forcing and land variables and other states (from model) will
  be sampled at all stations and be written to the databases.

  There are two basic database options in updating. One is for
  operation and the other is for archiving.
'''

#import argparse
import datetime as dt
import calendar
#import glob
import re
import sys
import os
import time
import errno
#import shutil
#import itertools
import sqlite3
#import math
import getpass
#import pyproj
import pandas as pd
#import matplotlib.pyplot as plt
#import psycopg2
import numpy as np
#from netCDF4 import Dataset, num2date
import wdb0

#sqlite3.register_adapter(np.int32, lambda val: int(val))
sqlite3.register_adapter(np.int32, int)


def string_to_datetime(date_str, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Convert a string time in the default format of %Y-%m-%d %H:%M:%S
    to a datetime unless a different date_format is given
    '''
    return dt.datetime.strptime(date_str, date_format)

def datetime_to_string(date_dt, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Convert a datetime time info to a string of %Y-%m-%d %H:%M:%S
    as default unless a different date_format is given.
    '''
    return dt.datetime.strftime(date_dt, date_format)

def datetime_to_utc_epoch(datetime_in):
    """
    Convert a (UTC) datetime into seconds since 1970-01-01 00:00:00.
    """
    return calendar.timegm(datetime_in.timetuple())


def utc_epoch_to_datetime(datetime_epoch):
    """
    Convert a (UTC) date/time expressed as seconds since 1970-01-01 00:00:00
    to a datetime type.
    """
    return \
        dt.datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') + \
        dt.timedelta(seconds=datetime_epoch)


def utc_epoch_to_string(datetime_epoch, date_format='%Y-%m-%d %H:%M:%S'):
    """
    Convert a (UTC) date/time expressed as seconds since 1970-01-01 00:00:00
    to a string in the default form "YYYY-MM-DD HH:MM:SS". If date_foramt
    is given, use the given format.
    """
    return dt.datetime.strftime(utc_epoch_to_datetime(datetime_epoch),
                                date_format)

def string_to_utc_epoch(date_str, date_format='%Y-%m-%d %H:%M:%S'):
    '''
    Convert a string time in the default format of %Y-%m-%d %H:%M:%S
    to utc_epoch seconds via a datetime unless a date_format is given
    '''
    date_dt = dt.datetime.strptime(date_str, date_format)
    return calendar.timegm(date_dt.timetuple())

def progress(count, total, status=''):
    """
    Progress bar:
    Copied from
    https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
    Modified: 1/6/2020
    """
    bar_len = 50
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    pbar = '=' * filled_len + '-' * (bar_len - filled_len)

    #sys.stdout.write('\r[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.write('\r[%s] %s%s %s\r' % (pbar, percents, '%', status))
    sys.stdout.flush()

def timedelta_to_int_hours(delta_time):

    """
    Converts a datetime timedelta object to an integer number of hours.
    """

    delta_time_hours = delta_time.days * 24 + \
                       delta_time.seconds // 3600
    # delta_time.total_seconds() // 3600 also works
    return delta_time_hours


def get_start_finish_date_from_name(db_file, oper):
    '''
    Get start and finish date info from the given database file.
    '''
    dates = re.findall('[0-9]{10}', db_file)
    if len(dates) > 0 and oper is False:
        db_start_date_from_name = dates[-2]
        db_start_datetime_from_name_dt = \
            dt.datetime.strptime(db_start_date_from_name, '%Y%m%d%H')
        db_start_datetime_from_name_ep = \
            datetime_to_utc_epoch(db_start_datetime_from_name_dt)
        db_finish_date_from_name = dates[-1]
        db_finish_datetime_from_name_dt = \
            dt.datetime.strptime(db_finish_date_from_name, '%Y%m%d%H')
        db_finish_datetime_from_name_ep = \
            datetime_to_utc_epoch(db_finish_datetime_from_name_dt)
    else:
        db_start_datetime_from_name_dt = None
        db_finish_datetime_from_name_dt = None
        db_start_datetime_from_name_ep = None
        db_finish_datetime_from_name_ep = None

    return db_start_datetime_from_name_ep, db_finish_datetime_from_name_ep


def attach_databases(conn,
                     forcing_single_db,
                     land_single_db):
    '''
    Attach companion land and forcing data databases.
    '''
    if os.path.isfile(forcing_single_db):
        conn.execute('ATTACH DATABASE "' + forcing_single_db + '" AS forcing_single')
    else:
        print('Database file {} does not exist. Need to create it first'.format(forcing_single_db))
        sys.exit(1)

    if os.path.isfile(land_single_db):
        conn.execute('ATTACH DATABASE "' + land_single_db + '" AS land_single')
    else:
        print('Database file {} does not exist. Need to create it first'.format(land_single_db))
        sys.exit(1)


def get_data_column_names(conn, land_layer):
    '''
    Get column names in the orginal order from data databases.
    '''
    tb_info = conn.execute("PRAGMA forcing_single. \
                            table_info('nwm_forcing_single_layer')").fetchall()
    column_names = list(zip(*tb_info))[1]
    forcing_single_layer_col_names = ','.join(column_names)

    tb_info = conn.execute("PRAGMA \
                            land_single.table_info('nwm_land_single_layer')") \
                            .fetchall()
    column_names = list(zip(*tb_info))[1]
    land_single_layer_col_names = ','.join(column_names)

    if len(land_layer) != 0:
        tb_info = conn.execute("PRAGMA \
                                land_snow.table_info('nwm_land_snow_layers')") \
                               .fetchall()
        column_names = list(zip(*tb_info))[1]
        land_snow_layer_col_names = ','.join(column_names)
    else:
        land_snow_layer_col_names = []

    return forcing_single_layer_col_names, \
           land_single_layer_col_names, \
           land_snow_layer_col_names

def get_swe_data(base_name_dir,
                 start_datetime_str,
                 finish_datetime_str,
                 time_format,
                 domain):
    '''
    get nwm and obs data either from reading csv files or querying from
    databases for the defined period and domain
    '''
    start_datetime = dt.datetime.strptime(start_datetime_str, time_format)
    finish_datetime = dt.datetime.strptime(finish_datetime_str, time_format)
    start_time_ep = datetime_to_utc_epoch(start_datetime)
    finish_time_ep = datetime_to_utc_epoch(finish_datetime)

    print('Data will be read from databases')

    db_dir, db_file = os.path.split(base_name_dir)
    db_path = base_name_dir

    if base_name_dir is None:
        print('Need to provide a base database name')
        sys.exit(1)

    #db_dir = opt.db_dir
    #db_file = opt.base_name
    #db_path = os.path.join(db_dir, db_file)

    if "_oper" in db_file:
        oper = True
    else:
        oper = False

    # Temporary file storage for observations read from the web database.
    scratch_dir = os.path.join('/net/scratch', os.getlogin())

    #db_dir, db_file = os.path.split(db_path)
    db_start_datetime_from_name_ep, \
    db_finish_datetime_from_name_ep = \
        get_start_finish_date_from_name(db_file, oper)

    # Open SQLite3 database file and do the checking
    try:
        sqldb_conn = sqlite3.connect(db_path,
                                     detect_types=sqlite3.
                                     PARSE_DECLTYPES|sqlite3.
                                     PARSE_COLNAMES)
        sqldb_cur = sqldb_conn.cursor()

        #set temp store directory to avoid database or disk is full issue
        #sqldb_conn.execute("PRAGMA temp_store_directory='/tmp'")
        sqldb_conn.execute("PRAGMA temp_store_directory='/disks/scratch'")
    except sqlite3.OperationalError:
        print('ERROR: Failed to open database file "{}".'.format(db_path),
              file=sys.stderr)
        sys.exit(1)

    #ATTACH RELATED DATABASES WHICH HOUSING DIFFERENT TYPES OF DATA
    #To check if required databases have been created

    #assumed names
    forcing_single_db_name = db_path.replace('_base', '_forcing_single')
    land_single_db_name = db_path.replace('_base', '_land_single')


    ##Now check if other companion databases are exist and then attach them
    #start_date_str = \
    #    utc_epoch_to_string(db_start_datetime_from_name_ep, '%Y%m%d%H')
    #finish_date_str = \
    #    utc_epoch_to_string(db_finish_datetime_from_name_ep, '%Y%m%d%H')

    attach_databases(sqldb_conn,
                     forcing_single_db_name,
                     land_single_db_name)

    print('Companion databases are now attached!')

    #Determine the period where data are available
    select_str = "SELECT datetime FROM land_single.nwm_land_single_layer " + \
                 "ORDER BY rowid DESC LIMIT 1"
    print('\nDatabase was for period from {} to {}'.format(
        utc_epoch_to_datetime(db_start_datetime_from_name_ep),
        utc_epoch_to_datetime(db_finish_datetime_from_name_ep)))
    db_latest_datetime_ep = sqldb_conn.execute(select_str).fetchone()[0]
    print('The actual data are available up to {}'.format(
        utc_epoch_to_datetime(db_latest_datetime_ep)))

    db_time_start = db_start_datetime_from_name_ep
    db_time_end = db_latest_datetime_ep

    if start_time_ep < db_time_start:
        print('Start date is earlier than database start date')
        sys.exit(1)
    if finish_time_ep > db_time_end:
        print('End date is later than the database end date')
        sys.exit(1)

    start_yyyymmddhh = start_datetime.strftime('%Y%m%d%H')
    finish_yyyymmddhh = finish_datetime.strftime('%Y%m%d%H')

    wdb_swe_df = wdb0.get_swe_obs_df(start_datetime,
                                     finish_datetime,
                                     scratch_dir=scratch_dir)
    print('Dimenssion of wdb data: ', wdb_swe_df.shape)

    wdb_swe_df['date'] = wdb_swe_df['date'].map(
        lambda element: datetime_to_utc_epoch(element))
    wdb_swe_df['date'] = wdb_swe_df['date'].map(
        lambda element: utc_epoch_to_datetime(element))
    wdb_swe_df = wdb_swe_df.rename(columns={'date':'datetime'})
    wdb_swe_fname = 'wdb_swe_' + start_yyyymmddhh + '_' + \
                    finish_yyyymmddhh + '.csv'
    wdb_swe_df.to_csv(wdb_swe_fname, encoding='utf-8', index=False)


    #sub_stations_df, nwm_swe_df = \
    nwm_swe_df = \
        get_nwm_data_from_db(sqldb_conn,
                             start_time_ep,
                             finish_time_ep,
                             domain)

    ##Write subsetted station data to a csv file
    #sub_stations_df.to_csv('sub_stations_df.csv', encoding='utf-8', index=False)

    sqldb_conn.close()

    return nwm_swe_df, wdb_swe_df
    #return nwm_swe_df, wdb_swe_df, sub_stations_df


def datetime_series_to_epoch(dts):
    '''From a single value of datetime series to epoch seconds'''
    #numpy.datetime64
    dt64 = pd.to_datetime(dts).values[0]
    #Timestamp
    dt_ts = pd.to_datetime(dt64)
    #datetime
    dtime = pd.to_datetime(dt_ts).to_pydatetime()
    datetime_ep = datetime_to_utc_epoch(dtime)
    return datetime_ep

def ymdh_from_epoch(dt_ep):
    '''extract year, month, day, and hour values from a epoch seconds'''
    year = time.gmtime(dt_ep).tm_year
    month = time.gmtime(dt_ep).tm_mon
    day = time.gmtime(dt_ep).tm_mday
    hour = time.gmtime(dt_ep).tm_hour
    return year, month, day, hour

def ymdh_from_datetime(dtime):
    '''extract year, month, day, and hour values from a datetime'''
    datetime_idx = pd.DatetimeIndex(dtime)
    year = datetime_idx.year[0]
    month = datetime_idx.month[0]
    day = datetime_idx.day[0]
    hour = datetime_idx.hour[0]
    return year, month, day, hour

def day_from_datetime(dtime):
    '''extract day value from a datetime'''
    datetime_idx = pd.DatetimeIndex(dtime)
    day = datetime_idx.day[0]
    return day

def hour_from_datetime(dtime):
    '''extract hour value from a datetime'''
    datetime_idx = pd.DatetimeIndex(dtime)
    hour = datetime_idx.hour[0]
    return hour


def get_nwm_data_from_db(sqldb_conn,
                         time_start,
                         time_end,
                         domain):
    '''Subset data for defined time and spatial domains '''

    ##Get stations for the subset domain
    #sub_stations_df = get_subset_stations(sqldb_conn, domain)

    #Get land variable data (SWE) for the subset domains
    data_df = get_subset_nwm_data(sqldb_conn, time_start, time_end, domain)

    ##intersect data
    #subset_data_df = data_df.merge(sub_stations_df, on=['obj_identifier'])
    #print('Dimension of intersected data:', subset_data_df.shape)

    #return sub_stations_df, data_df, subset_data_df
    #return sub_stations_df, data_df
    return data_df

def get_subset_stations(sqldb_conn, domain):
    '''Get subset station info as data frame'''
    select_stations = "SELECT obj_identifier, longitude, latitude, " +\
                      "nwm_grid_column, nwm_grid_row " + \
                      "FROM stations WHERE " + \
                      "longitude <=" + str(domain['max_lon']) +" AND " + \
                      "longitude >=" + str(domain['min_lon']) +" AND " + \
                      "latitude <=" + str(domain['max_lat']) +" AND " + \
                      "latitude >=" + str(domain['min_lat'])
    sub_stations = sqldb_conn.execute(select_stations).fetchall()
    sub_stations_df = pd.DataFrame(sub_stations,
                                   columns=['obj_identifier',
                                            'longitude',
                                            'latitude',
                                            'nwm_grid_col',
                                            'nwm_grid_row'])
    print('Dimension of subset stations info:', sub_stations_df.shape)
    return sub_stations_df

def get_subset_nwm_data(sqldb_conn, time_start, time_end, domain):
    '''Get subset nwm swe data as data frame'''

    data_select_str = \
        " SELECT stations.obj_identifier, " + \
        "     land_single.nwm_land_single_layer.datetime, " + \
        "     land_single.nwm_land_single_layer.nwm_snow_water_equivalent " + \
        " FROM stations, land_single.nwm_land_single_layer," + \
        "     nwm_file_update_info " + \
        " WHERE nwm_file_update_info.is_reference = 1 " + \
        " AND nwm_file_update_info.nwm_group = 'land' " + \
        " AND stations.longitude <=" + str(domain['max_lon']) + \
        " AND stations.longitude >=" + str(domain['min_lon']) + \
        " AND stations.latitude <=" + str(domain['max_lat']) + \
        " AND stations.latitude >=" + str(domain['min_lat']) + \
        " AND land_single.nwm_land_single_layer.datetime >=" + str(time_start) + \
        " AND land_single.nwm_land_single_layer.datetime <="  + str(time_end) + \
        " AND nwm_file_update_info.datetime = " + \
        "     land_single.nwm_land_single_layer.datetime" + \
        " AND nwm_file_update_info.cycle_datetime = " + \
        "     land_single.nwm_land_single_layer.cycle_datetime" + \
        " AND nwm_file_update_info.cycle_type = " + \
        "     land_single.nwm_land_single_layer.cycle_type " + \
        " AND stations.obj_identifier = " + \
        "     land_single.nwm_land_single_layer.station_obj_identifier " + \
        "ORDER BY stations.obj_identifier, land_single.nwm_land_single_layer.datetime"
        #"nwm_file_update_info.is_reference FROM stations," + \

    print(data_select_str)
    try:
        data = sqldb_conn.execute(data_select_str).fetchall()
    except:
        print('Data query failed')
        sys.exit(1)
    data_df = pd.DataFrame(data, columns=['obj_identifier',
                                          'datetime',
                                          'swe'])
                                          #'is_reference'])
    print('Dimension of queried NWM data:', data_df.shape)

    #Reset index and convert the column datetime to datetime type
    # and write data to a csv file
    data_df = data_df.reset_index(drop=True)
    data_df['datetime'] = data_df['datetime'].map(
        lambda element: utc_epoch_to_datetime(element))
    start_datetime_str = utc_epoch_to_string(time_start)
    finish_datetime_str = utc_epoch_to_string(time_end)
    start_yyyymmddhh = start_datetime_str[0:10].replace('-', '') + \
                       start_datetime_str[11:13]
    finish_yyyymmddhh = finish_datetime_str[0:10].replace('-', '') + \
                        finish_datetime_str[11:13]
    nwm_swe_fname = 'nwm_swe_' + start_yyyymmddhh + '_' + \
                    finish_yyyymmddhh + '.csv'
    data_df.to_csv(nwm_swe_fname, encoding='utf-8', index=False)
    return data_df


def change_datetime_to_readable_string(datetime_df,
                                       column_name):
    '''
    Change time column of a dataframe in epoch seconds
    to readable time string in yyyy-mm-dd hh:00
    '''
    str_list = []
    for each_v in datetime_df:
        each_str = utc_epoch_to_string(each_v, '%Y-%m-%d %H:%M')
        str_list.append(each_str)
    str_series = pd.Series(str_list)
    df_str = str_series.to_frame(column_name)
    datetime_df = df_str
    return datetime_df

def get_station_latlon_obj_ids_times(conn, db_file):
    '''
    Get station lat/lon and obj_identifier info from stations
    '''
    # Read NWM grid row/column of stations.
    try:
        db_grid_cols = conn.execute('SELECT nwm_grid_column FROM stations').fetchall()
        db_grid_rows = conn.execute('SELECT nwm_grid_row FROM stations').fetchall()
    except:
        print('ERROR: Failing to get grid col/row data from ' +
              db_file + '.', file=sys.stderr)

    # Get obj_identifier of all stations.
    try:
        obj_ids = conn.execute("SELECT obj_identifier FROM stations").fetchall()
        #print('GF03 ', type(obj_ids[0]))

    except:
        print('ERROR: Failing to get obj_identifier data from ' +
              db_file + '.', file=sys.stderr)

    # Get stop_datetime and start_datetime for each station
    try:
        db_start_dates_ep = conn.execute("SELECT start_date" + \
                                         " FROM stations").fetchall()
        db_stop_dates_ep = conn.execute("SELECT stop_date" + \
                                         " FROM stations").fetchall()
    except:
        print('ERROR: Failing to get start/stop dates from ' +
              db_file + '.', file=sys.stderr)

    return db_grid_cols, db_grid_rows, obj_ids, \
           db_start_dates_ep, db_stop_dates_ep


def py_get_data_main(start_datetime_str,
                     finish_datetime_str,
                     db_base_name):

    """
    Compare observed and NWM snow water equivalent.
    """


    #Get initial raw data (nwm_swe and wdb_swe)

    #csv_dir = '/nwcdev/nwm_da/m3_dev'
    #db_base_name = '/disks/scratch/zzhang/m3db/western_us/nwm_ana_station_neighbor_archive_2019100100_to_2020053123_base.db'
    #Domain: Western USA
    domain = {'min_lat': 30.0, 'max_lat': 50.0,
              'min_lon': -125.0, 'max_lon': -100.0}
    #Domain: estern USA - info from Aubrey's R file
    domain = {'min_lat': 31.0, 'max_lat': 51.0,
              'min_lon': -126.0, 'max_lon': -101.0}
    # Define a time period to examine.
    #start_datetime_str = '2019-12-01 12:00:00'
    #finish_datetime_str = '2019-12-31 12:00:00'
    #start_datetime_str = '2019-10-01 12:00:00'
    #finish_datetime_str = '2020-02-29 12:00:00'
    time_format = '%Y-%m-%d %H:%M:%S'

    start_yyyymmddhh = start_datetime_str[0:10].replace('-', '') + \
                       start_datetime_str[11:13]
    finish_yyyymmddhh = finish_datetime_str[0:10].replace('-', '') + \
                        finish_datetime_str[11:13]
    print('start=', start_yyyymmddhh)
    print('finish=', finish_yyyymmddhh)
    print('Getting data from databases via python ...')
    sys.stdout.flush()  #try to force print in R
    #nwm_swe_df, wdb_swe_df, sub_stations_df = \
    nwm_swe_df, wdb_swe_df = \
        get_swe_data(db_base_name,
                     start_datetime_str,
                     finish_datetime_str,
                     time_format,
                     domain)
    return nwm_swe_df, wdb_swe_df


