import configparser
import os
import pathlib
import pickle
from pathlib import PurePath
from typing import Optional, IO, Any
import numpy as np
import pandas as pd
from pandas._typing import DtypeArg
import io
import yaml
from aws_s3 import AwsS3
from joblib import load

def parse_config_dictionary(path, s3) -> dict:
    if s3 is not None:
        tmp_file = s3.read_file_obj(PurePath(path), callback=None)
        if tmp_file is not None:
            try:
                config = parse_config_dictionary_file(tmp_file.name)
            finally:
                tmp_file.delete = True
                tmp_file.close()
            return config
        else:
            return {}
    else:
        return parse_config_dictionary_file(path)


def load_file_by_type(input_path: str, s3: AwsS3, type: str):
    if s3 is not None:
        tmp_file = s3.read_file_obj(PurePath(input_path), callback=None)
        if tmp_file is not None:
            try:
                if type == 'joblib':
                    file_loaded = load_joblib_file(tmp_file.name)
                elif type == 'pickle':
                    file_loaded = load_pickle_file(tmp_file.name)
                elif type == 'csv':
                    file_loaded =  load_csv_file(tmp_file.name)
            finally:
                tmp_file.delete = True
                tmp_file.close()
            return file_loaded
        else:
            return None
    else:
        file_loaded = None
        if type == 'pickle':
            file_loaded = load_pickle_file(input_path)
        elif type == 'joblib':
            file_loaded = load_joblib_file(input_path)
        elif type == 'csv':
            file_loaded = load_csv_file(input_path)

        return file_loaded

def load_csv_file(input_path):
    return pd.read_csv(input_path)


def load_joblib_file(input_path):
    return load(input_path)


def load_pickle_file(input_path):
    with open(input_path, 'rb') as input_file:
        return load_pickle_file(input_file)


def parse_config_dictionary_file(config_file_path):
    print(f'parsing config dictionary from: {config_file_path}', end='')
    try:
        config_parser = configparser.RawConfigParser()
        files = config_parser.read(config_file_path)
        print(f'   done - loaded: {files}')
    except Exception as e:
        print(f'\n ERROR: config file read failed with: {e} ')
        return {}

    config_dict = {}
    for section in config_parser.sections():
        config_dict[section] = {}
        for item in config_parser.items(section):
            if (item[1][0] == '[') & (item[1][-1] == ']'):
                list_ = item[1] \
                    .replace('[', '').replace(']', '').replace('\n', '') \
                    .replace("'", '').replace(" ", '').split(',')
                config_dict[section][item[0]] = list_
            else:
                if (item[1][0] == '{') & (item[1][-1] == '}'):
                    dict_string = item[1] \
                        .replace('{', '').replace('}', '').replace('\n', '') \
                        .replace("'", '').replace(" ", '').split(',')
                    dict_ = {item.split(':')[0]: item.split(':')[1] for item in dict_string}
                    config_dict[section][item[0]] = dict_
                else:
                    if item[1] == str(True):
                        config_dict[section][item[0]] = True
                    else:
                        if item[1] == str(False):
                            config_dict[section][item[0]] = False
                        else:
                            config_dict[section][item[0]] = item[1]

    if len(config_dict) == 0:
        raise Exception(f'config file read failed from path = {config_file_path}')

    return config_dict


def parse_yaml_from_dir(yaml_dir):
    a_yaml_file = io.open(yaml_dir)
    parsed_yaml_file = yaml.load(a_yaml_file, Loader=yaml.FullLoader)
    return parsed_yaml_file


def s3_key(s3_dir, output_filename):
    return PurePath(os.path.join(s3_dir, output_filename)).as_posix()


def save_data(data, path: str, s3):
    pure_path = PurePath(path)
    output_dir = pure_path.parent.as_posix()
    save_data_s3(data, output_dir, pure_path.name, s3, output_dir)


def save_data_s3(data, output_dir: str, output_filename: str, s3, s3_dir: str):
    # todo check if file is locked
    # todo type == pd.DataFrame(), not empty. write to log
    local_file = save_data_file(data, output_dir, output_filename)

    # Upload the file to S3
    if s3 is not None:
        s3.put_keyed_file(local_file, s3_key(s3_dir, output_filename), callback=None)


def save_data_file(data, output_dir: str, output_filename: str):
    local_file = os.path.join(output_dir, output_filename)
    print(f'Saving data to {output_dir}/{output_filename}')
    if not os.path.exists(output_dir):
        print(f'  path {output_dir} does not exist - creating.', end='')
        try:
            path = pathlib.Path(output_dir)
            path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f'\n ERROR: failed to create directory {output_dir} error =: {e} ')
            return

    data.to_csv(local_file)
    print(f'...completed \n')
    return local_file


def save_pickle(model, output_dir: str, output_filename: str, s3, s3_dir: str):
    local_file = os.path.join(output_dir, output_filename)
    print(f'saving file as pickle to {local_file}')
    pickle.dump(model, open(local_file, 'wb'))
    print('...completed \n')

    # Upload the file to S3
    if s3 is not None:
        s3.put_keyed_file(local_file, s3_key(s3_dir, output_filename), callback=None)


def s3_file_upload_callback():
    pass


def load_pickle(input_path: str, s3):
    if s3 is not None:
        tmp_file = s3.read_file_obj(PurePath(input_path), callback=None)
        if tmp_file is not None:
            try:
                with open(tmp_file.name, 'rb') as input_file:
                    config = load_pickle_file(input_file)
            finally:
                tmp_file.delete = True
                tmp_file.close()
            return config
        else:
            return None
    else:
        with open(input_path, 'rb') as input_file:
            return load_pickle_file(input_file)


def load_pickle_file(input_file: IO[Any]) -> dict:
    dict_ = pickle.load(input_file)
    print(f'pickle load from {input_file.name}')
    return dict_


def date_unix_time(date_time: str):
    return np.datetime64(date_time).astype('datetime64[ms]').astype('int64').astype(float)


def load_file(input_path: str, feature_date_name, s3, types_dict=None, date_structure=None):
    print(f'start loading file: {input_path}')
    data = pd.read_csv(input_path, s3, index_col=False, dtype=types_dict)
    # todo can we handle the index with dtype during the loading?
    data[feature_date_name] = data[feature_date_name].astype(np.datetime64)
    print('...Parsing dates done \n')
    data.set_index(feature_date_name, inplace=True)
    print(f'...file loaded. shape =  {data.shape} \n')
    return data


def load_csv(input_path: str, s3, index_col=False, dtype: Optional[DtypeArg] = None):
    if s3 is not None:
        tmp_file = s3.read_file_obj(PurePath(input_path), callback=None)
        if tmp_file is not None:
            try:
                config = load_csv_file(tmp_file.name, index_col, dtype)
            finally:
                tmp_file.delete = True
                tmp_file.close()
            return config
        else:
            return None
    else:
        return load_csv_file(input_path, index_col, dtype)


def load_csv_file(input_path, index_col=False, dtype: Optional[DtypeArg] = None):
    print(f'start loading file: {input_path}')
    data = pd.read_csv(input_path, index_col=index_col, dtype=dtype)
    print(f'...file loaded. shape =  {data.shape} \n')
    return data



