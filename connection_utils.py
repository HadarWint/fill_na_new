import os
from datetime import timezone
import eland as ed
import numpy as np
from elasticsearch_dsl import connections
from sqlalchemy import create_engine
import pandas as pd

def clean_nan_columns_from_es_table(input_df):
    """
    removes columns with nans only
    :returns pd.DataFrame
    """
    return input_df.drop(input_df.loc[:, input_df.isnull().sum() == input_df.shape[0]].columns, axis=1)

def postgresql_connection():
    # todo move to config
    db_host = os.environ.get('PEPSICO_PREDICTOR_SVC_POSTGRES_HOST')
    db_port = os.environ.get('PEPSICO_PREDICTOR_SVC_POSTGRES_PORT')
    db_name = os.environ.get('PEPSICO_PREDICTOR_SVC_POSTGRES_DB_NAME')
    db_user = os.environ.get('PEPSICO_PREDICTOR_SVC_POSTGRES_USER')
    db_pw = os.environ.get('PEPSICO_PREDICTOR_SVC_POSTGRES_PW')

    alchemy_engine = create_engine(
        f'postgresql+psycopg2://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}', pool_recycle=3600,echo=False)

    # Connect to PostgreSQL server
    db_connection = alchemy_engine.connect()
    return db_connection


def find_client(host):
    client = connections.create_connection(hosts=[{
        'host': host,
        'port': 443,
        'use_ssl': True
    }])
    return client


def find_data_table(client, es_index_pattern):
    """
    Lazy data frame
    """
    eland_df = ed.DataFrame(es_client=client, es_index_pattern=es_index_pattern)
    return eland_df

def find_data_table_drinks(client, es_index_pattern, cols=None):
    """
    Lazy data frame
    """
    eland_df = ed.DataFrame(es_client=client, es_index_pattern=es_index_pattern)
    if cols is not None:
        eland_df = eland_df[cols]
    else:
        col = [c for c in eland_df.columns if
               'PC4' not in c and 'murcia_utilities monitoring' not in c and 'murcia_pasteuiser_tetra' not in c
               and 'PC3' not in c and 'PC1' not in c]
        eland_df = eland_df[col]
    return eland_df

def date_utc_time(date):
    utc_time = date.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    return utc_timestamp

def date_unix_time(date_time: str):
    return np.datetime64(date_time).astype('datetime64[ms]').astype('int64').astype(float)

def retrieve_dataframe_from_es_pc(start_date_str: str, end_date_str: str, es_data_table,
                               feature_date_name_col, local_date_name_col, site_name):

    start_epoch = date_unix_time(start_date_str)
    end_epoch = date_unix_time(end_date_str)
    # start_epoch = date_utc_time(start_date_str)
    # end_epoch = date_utc_time(end_date_str)
    if not 'cip' in site_name:
        col = [c for c in es_data_table.columns if 'TC_WINT' not in c and 'murcia_utilities monitoring' not in c and 'murcia_pasteuiser_tetra' not in c and 'TC11' not in c and 'TC11' not in c and  'record.pst' not in c  and  'record.ut' not in c]
        es_data_table = es_data_table[col]
    print('\n', end_epoch, start_epoch)
    df = ed.eland_to_pandas(
        es_data_table[(es_data_table['timestampLongUtc'] >= start_epoch) & (
                              es_data_table['timestampLongUtc'] <= end_epoch)])
    # x = es_data_table[(es_data_table['timestampLongUtc'] <= 1646335023000)]
    # r_data = es_data_table[(es_data_table['timestampLongUtc'] >= int(start_epoch)) & (
    #                           es_data_table['timestampLongUtc'] <= int(end_epoch))]
    # df = ed.eland_to_pandas(r_1.tail(1000))
    print(f'data set shape is {df.shape} ')
    assert not df.empty, f'Error! data frame from Elastic Search is Empty'
    if 'timestamp_long_utc' in df.columns:
        df = df.rename(columns={'timestamp_long_utc': 'timestampLongUtc'})
    df = clean_nan_columns_from_es_table(df)
    columns = [c.replace('record.', '') for c in df.columns]
    df.columns = columns
    # df.index = utils.parse_dates(df[feature_date_name_col])
    # todo raise exception for empty DF
    df.index = df[feature_date_name_col]
    df["local_date_time"] = df[local_date_name_col]
    df.sort_index(inplace=True)
    if not 'cip' in site_name:
        df.drop([feature_date_name_col, local_date_name_col], axis=1, inplace=True)
    else:
        df.drop(['timestampLong', 'timestamp', '_class',
                 'created', 'line', 'productionLineId', 'recordSourceId'],
                axis=1, inplace=True, errors='ignore')
    print('import from ES completed successfully')
    return df


def retrive_dataframe_from_es(start_date_str: str, end_date_str: str, es_data_table):
    start_epoch = date_unix_time(start_date_str)
    end_epoch = date_unix_time(end_date_str)

    print('\n', end_epoch, start_epoch)
    df = ed.eland_to_pandas(
        es_data_table[(
                              es_data_table[
                                  'timestampLongUtc'] >= int(
                          start_epoch)) & (
                              es_data_table[
                                  'timestampLongUtc'] <= int(
                          end_epoch))])

    print(f'data set shape is {df.shape}')
    assert not df.empty, f'Error! data frame from Elastic Search is Empty'
    columns = [c.replace('record.', '') for c in df.columns]
    df.columns = columns
    # todo raise exception for empty DF
    #
    # df.sort_index(inplace=True)
    df.drop(['timestampLong', 'timestamp', '_class',
             'created', 'line', 'productionLineId', 'recordSourceId'],
            axis=1, inplace=True, errors='ignore')
    return df


def retrieve_data_low_level(client, index, start, end, freq, lim):
    dates = pd.date_range(start, end, freq=freq)
    length = len(dates)
    if length > lim:
        print('retrieving data in parts')
        data = pd.DataFrame()
        ser = np.arange(0, length, lim, dtype=int)
        len_ser = len(ser)
        for s in range(len_ser):
            start_epoch = dates[ser[s]]
            if s + 1 == len_ser:
                end_epoch = dates[-1]

            else:
                end_epoch = dates[ser[s + 1]]
            print(f'retrieve data part from {start_epoch} to {end_epoch}', end=',')
            start_epoch = date_unix_time(start_epoch)
            end_epoch = date_unix_time(end_epoch)
            res = client.search(index=index, body={"query": {"bool": {
                "must": [{"range": {"timestampLongUtc": {"gte": f"{start_epoch}", "lt": f"{end_epoch}"}}}],
                "must_not": [], "should": []}}, "from": 0, "size": 10000, "sort": [], "aggs": {}})
            if len(res['hits']['hits']) > 0:
                template = pd.DataFrame.from_dict(res['hits']['hits'])['_source']
                temp2 = pd.json_normalize(template[:])
                data = pd.concat([data, temp2])
    else:
        start_epoch = date_unix_time(start)
        end_epoch = date_unix_time(end)
        res = client.search(index=index, body={"query": {"bool": {
            "must": [{"range": {"timestampLongUtc": {"gte": f"{start_epoch}", "lt": f"{end_epoch}"}}}],
            "must_not": [], "should": []}}, "from": 0, "size": 10000, "sort": [], "aggs": {}})
        if len(res['hits']['hits']) > 0:
            template = pd.DataFrame.from_dict(res['hits']['hits'])['_source']
            data = pd.json_normalize(template[:])
    print('retrieving data is done!')
    return data


def clean_record(data):
    for c in data.columns:
        data.rename(columns={c: c.replace('record.', '')}, inplace=True)
    return data
