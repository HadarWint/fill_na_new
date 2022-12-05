import pandas as pd
import numpy as np
from visualize_nulls import Visualize_nulls
from email_service import EMailService
import os
from pathlib import Path as path
from datetime import timedelta
import io_utils
import connection_utils
from aws_s3 import AwsS3
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
import re
import time

class ES_to_S3:
    def __init__(self, site, start, end, data_repo, config, offline=False):
        self.site_txt = site
        self.data_repo = os.path.join(str(path.home()), data_repo)
        self.config = config
        self.start = start
        self.end = end
        self.filter_column = 'daily'
        self.host = config['es_connection']['host']
        self.es_index_pattern = config['es_connection']['es_index_pattern']
        self.time_categorical = config['pre_p']['time_hold_categorical']
        if 'pc' in self.site_txt:
            self.unneeded_columns_es = config['general']['unneeded_columns_es']
            self.feature_date_name_col = config['general']['feature_date_name_col']
            self.datetime_feature_name = self.feature_date_name_col
            self.production_line = config['es_connection']['es_production_line']
            self.low_level = False
            self.site = config['general']['production_line']
            self.time_categorical = config['pre_p']['time_hold_categorial']
            self.product_name = None
            self.local_date_name_col = config['general']['local_date_name_col']
        if 'tc' in self.site_txt:
            self.datetime_feature_name = config['general']['datetime_feature']
            self.site = config['general']['site']
            self.local_time_feature = config['general']['local_time_feature']
            self.local_date_time = self.local_time_feature
            self.feature_date_name_col = self.datetime_feature_name
            self.low_level = config['es_connection']['low_level']
            self.product_name = config['pre_p']['product_type']
            self.production_line = None
        if 'cip' in self.site_txt:
            self.datetime_feature_name = config['general']['datetime_feature']
            self.feature_date_name_col = self.datetime_feature_name
            self.local_date_name_col = self.feature_date_name_col
            self.low_level = None
            self.filter_column = 'daily'
            self.product_name = config['general']['source']
            self.production_line = None
        self.kpi_categorical = config['pre_p']['categorical_feature']
        self.kpis = self.kpi_categorical
        if self.low_level:
            self.es_freq = config['es_connection']['es_freq']
            self.rows_limit = config['es_connection']['rows_limit']
        self.client = None
        self.eland_df = None
        self.offline = offline
        self.flow_meters = config['pre_p']['flow_meter_na_completion']
        self.max_seconds = config['pre_p']['max_seconds_na']
        self.bucket_name = config['s3']['op_bucket']
        # self.interpolation_methods = ['ffill','linear','SMM','spline']
        self.s3 = True
        self.save_raw_files_locally = False
        self.save_locally_from_s3 = True

    def remove_unneeded_columns_from_df(self, df):
        """
        :param df:
        :return:
        """
        for c in self.unneeded_columns_es:
            if c not in df.columns:
                print(f'Warning! on def remove_unneeded_columns_from_df:  trying to drop {c} however it does not exist')
                self.unneeded_columns_es.remove(c)
        print(f'Dropping {len(self.unneeded_columns_es)} col from ingested data: \n {self.unneeded_columns_es} ')
        return df.drop(self.unneeded_columns_es, axis=1)

    def collect_data_from_es_low_level(self):
        """

        :return:
        """
        print(f'reading data from ES from {self.start} to {self.end} UTC', end='')
        if not self.client:  # if self.client is None than create new instance
            self.client = connection_utils.find_client(self.host)
        data = connection_utils.retrieve_data_low_level(self.client, self.es_index_pattern, self.start, self.end,
                                                        self.es_freq, float(self.rows_limit))
        data.set_index(self.datetime_feature_name, inplace=True, drop=False)
        data.sort_index(inplace=True)
        return data.drop(columns=['timestampLong', 'timestampLongUtc', 'timestamp', '_class',
                                  'created', 'line', 'productionLineId', 'recordSourceId'], errors='ignore')

    def collect_data_from_es(self, start_date_for_es, end_date, override_start=True):
        # todo if client exist- use it and dont create new instance
        if not self.client:  # if self.client is None than create new instance
            self.client = connection_utils.find_client(self.host)
        print(f'client created: {self.client}')
        if not self.eland_df:
            # if self.eland_df is None than create new instance
            if 'cip' not in self.site_txt:
                self.eland_df = connection_utils.find_data_table(self.client, self.es_index_pattern)
            else:
                self.eland_df = connection_utils.find_data_table_drinks(self.client, self.es_index_pattern)
        print(f'connection created')
        if override_start:
            start_date_for_es = np.datetime64(self.end, 'ms') - np.timedelta64(35, 'm')
            print('replacing start date with end date minus 35 minutes:')
        print(f'reading data from ES from {start_date_for_es} to {end_date} UTC', end='')
        data = connection_utils.retrieve_dataframe_from_es_pc(start_date_for_es,end_date , self.eland_df,
                                                            self.feature_date_name_col, self.local_date_name_col,
                                                              self.site_txt)
        if 'pc' in self.site_txt:
            data = self.remove_unneeded_columns_from_df(data)
        data.sort_index(inplace=True)
        print(f'  Ingestion of data from {self.production_line} Done \n Final data shape = {data.shape} \n')
        return data

    def get_data(self):
        """
        function that connects to es, recieves from which site to download data
        :return:  data from es
        """
        print(f'initiating loading data for site {self.site_txt} and timing the downloading function')
        t0 = time.time()
        if self.low_level:
            df = self.collect_data_from_es_low_level()
            df = connection_utils.clean_record(df)
        else:
            pipeline_start_date = np.datetime64(self.start)
            pipeline_end_date = np.datetime64(self.end)
            date_limit = True
            start_date_for_es = pipeline_start_date
            while date_limit:
                end_date = start_date_for_es + np.timedelta64(1, 'D')
                try:
                    df = self.collect_data_from_es(str(start_date_for_es).replace('T', ' '), str(end_date).replace('T', ' '),
                                                     override_start=False)
                except Exception as e:
                    print(f'\n ERROR: Failed to ingest: {start_date_for_es} to {end_date}')
                    print(e)
                finally:
                    start_date_for_es = end_date
                    if end_date > pipeline_end_date:
                        date_limit = False
        t1 = time.time()
        print(f'downloading speed per day is {t1 - t0} seconds')
        return df

    def remove_time_columns_regex(self, data):
        """
        :param data: Recieves df downloaded from ES
        :return: df without timestamp
        """
        cols = np.array(data.columns, dtype=str)
        time_cols = list(map(lambda x: x.find('timestamp') >= 0, list(cols)))
        remove_cols = list(cols[time_cols])
        remove_cols.remove(self.datetime_feature_name)
        return data.drop(columns=remove_cols)

    def sort_and_filter_data(self, data):
        """
        :data: the data downloaded for a day from es
        :return: data from defined timeline
         """
        print('arranging data and filtering it for ingestion')
        dates_range = pd.DataFrame()
        data[self.datetime_feature_name] = data[self.datetime_feature_name].astype('datetime64')
        data[self.filter_column] = data[self.datetime_feature_name].dt.date
        dates_range['dates'] = pd.date_range(self.start, self.end, freq='H')
        dates_range[self.filter_column] = dates_range['dates'].dt.date
        data[self.filter_column] = np.isin(data[self.filter_column], dates_range[self.filter_column])
        data = data.loc[data[self.filter_column] == True]
        data['year'] = data[self.datetime_feature_name].dt.year
        data['month'] = data[self.datetime_feature_name].map(lambda x: x.strftime('%m'))
        data['day'] = data[self.datetime_feature_name].map(lambda x: x.strftime('%d'))
        data['hour'] = data[self.datetime_feature_name].map(lambda x: x.strftime('%H'))
        return data.drop(columns=[self.filter_column, self.datetime_feature_name])

    # def generate_simulation(self,data_index,k):
    #     idx = random.sample(data_index, 300)
    #     # return idx
    #     res = []
    #     for i, v in enumerate(idx):
    #         res.append(data_index[v:v + k])
    #     return [item for sublist in res for item in sublist]

    # def visualize_residuals(self,data,column_name,k ):
    #     color = ['green','blue','red','grey']
    #     if not os.path.exists(f'C:\\Users\\HadarHolan\\Pictures\\residuals_of_nans\\{column_name}'):
    #         os.makedirs(f'C:\\Users\\HadarHolan\\Pictures\\residuals_of_nans\\{column_name}')
    #     for i,col in enumerate(self.interpolation_methods):
    #         sns.distplot(data.ground_truth - data[f'{col}'], hist=True, kde=False, bins=50, color=color[i],
    #                      hist_kws={'edgecolor': 'black'})
    #         plt.title(f'{col}_{column_name}_{k}')
    #         plt.xlabel('residual value')
    #         plt.ylabel('frequency')
    #         plt.savefig(f'C:\\Users\\HadarHolan\\Pictures\\residuals_of_nans\\{column_name}\\{col}_{k}.png')
    #         plt.show()

    # def filling_methods(self, data_col, data_syn_col,k, idx_to_compare,col):
    #     results = []
    #     residuals = []
    #     ground_truth =  data_col.iloc[idx_to_compare,:]
    #     data_syn_col['ffill'] =  data_syn_col[f'{col}'].ffill()
    #     data_syn_col['linear'] = data_syn_col[f'{col}'].interpolate(option='linear')
    #     data_syn_col['SMM'] = data_syn_col[f'{col}'].rolling(window=k,min_periods=1).median().ffill()
    #     data_syn_col['spline'] =  data_syn_col[f'{col}'].interpolate(option='spline')
    #     data_syn_col['ground_truth'] = data_col[f'{col}']
    #     groups_df  = data_syn_col.copy().reset_index()
    #     groups_df['group'] = ((groups_df['index']-groups_df['index'].shift()) > 1).cumsum()
    #     groups_df = groups_df.set_index('index')
    #     data_syn_col['groups_of_seq'] = data_syn_col.join(groups_df['group'])['group']
    #     test_section = data_syn_col.iloc[idx_to_compare,:]
    #     data_syn_col_copy = data_syn_col.copy()
    #     # self.visualize_residuals(data_syn_col, col,k)
    #     for col_name in self.interpolation_methods:
    #         results.append([col,k, col_name,test_section[f'{col_name}'].eq(test_section.ground_truth).mean()] )
    #
    #     return results, data_syn_col.linear

    # def cnan(self,s):
    #     v = s.values
    #     k = v.size
    #     n = np.append(np.isnan(v), False)
    #     m = np.empty(k, np.bool8)
    #     m.fill(True)
    #     i = np.where(n[:-1] & n[1:])[0] + np.arange(5)
    #     m[i[i < k]] = False
    #     return m
    #
    # def fill_under_n_sec_with_optimization(self, data):
    #     tot_accuracy = pd.DataFrame([])
    #     max_seconds_accuracy = []
    #     data = data.reset_index()
    #     r_data = data[self.flow_meters].copy()
    #     # r_data = r_data.dropna().reset_index()
    #     for sec_sample in self.max_seconds:
    #         # list_of_indexes = self.generate_simulation(r_data.index.to_list(), int(sec_sample))
    #         for col in self.flow_meters:
    #             condition = r_data[col].isnull()
    #             syn_null_df = pd.DataFrame(r_data[col]).copy().reset_index()
    #             syn_null_df['group'] = ((syn_null_df.timestampUtc - syn_null_df.timestampUtc.shift()).dt.total_seconds() > 1).cumsum()
    #             temp, data_with_linear_interpolation_column = self.filling_methods(pd.DataFrame(r_data[col]), syn_null_df,  int(sec_sample), col)
    #             temp = pd.DataFrame(temp, columns = ['column_name', 'time_filling', 'filling_method' ,'accuracy'])
    #             tot_accuracy = tot_accuracy.append(temp, ignore_index=True)
    #             data[f'{col}_competed'] = data_with_linear_interpolation_column
    #     data = data.set_index('timestampUtc')
    #     return  data

    def fill_if_previous_equal_next(self,non_flow_meter_completion_set,data):
        '''recieves a data built from 1 column and a set containing the indices the data was completed'''
        data2 = pd.DataFrame(data).copy()
        col = list(data2.columns)[0]
        data2[col] = data2[col].astype(float)
        data2[self.datetime_feature_name] = data2.index
        data2['gr'] = np.cumsum((np.abs(data2[col].isna().diff()) > 0) | (np.abs(data2[col].diff()) > 0))
        gr = data2.groupby('gr').first()
        gr['count'] = data2.groupby('gr').count()[self.datetime_feature_name]
        gr[col] = gr[col].astype(float)
        gr['shift_ahead'] = gr[col].shift(-1)
        gr['shift_prev'] = gr[col].shift(1)
        gr['replace'] = (gr[col].isna()) & (gr['shift_ahead'] == gr['shift_prev']) & (
                gr['count'] <= float(self.max_seconds[0]))
        gr['to_replace'] = gr[col]
        gr.loc[gr['replace'], 'to_replace'] = gr.loc[gr['replace'], 'shift_prev']
        if gr[gr['to_replace'].isna().eq(1)].shape[0] > 0:
            gr.loc[gr['to_replace'].isna(), 'to_replace'] = gr.loc[gr['replace'], 'shift_prev']
        non_flow_meter_completion_set.update(list(data2[data2.gr.isin(list(gr.loc[gr['replace'] == True].index))].index))
        data2 = data2.join(gr['to_replace'], on='gr')
        data2['to_replace'].ffill(inplace=True)
        data2 = data2.sort_index()
        return data2, non_flow_meter_completion_set

    def fill_under_n_sec(self, data):
        """
        :param data: takes columns that have a missing numerical value and solves
        2 cases:
        discrete columns - completes missing values if values before and after null are identical
        continous columns - linear manipulation to columns with missing values
        :return: a df with the completed nulls and columns indicating whether any element in row was completed
        """
        modified_indices_total = set()
        modified_indices_flow_meters = set()
        o_data = data.copy()
        valves = list(filter(re.compile(".*_[v,V][0-9]").match, list(data.columns)))
        other_col = data[valves].copy()
        r_cols = [col for col in data.columns if 'modified_categorical_row' not in col]
        r_data = data[r_cols]._get_numeric_data().copy()
        data['modified_cell'] = 0
        data['flow_meters_modified'] = 0
        r_data_m = r_data.copy()
        for col in r_data[r_data.columns.difference(valves)].columns:
            df = pd.DataFrame(r_data[[col]])
            df['original_index'] = pd.to_datetime(df.index)
            null_df = df[df[col].isna().eq(1)].copy()
            null_df['group'] = np.cumsum(np.absolute((null_df['original_index'] - null_df['original_index'].shift()).dt.total_seconds())>1)
            mask = null_df.groupby('group')['group'].transform('count') >= int(self.max_seconds[0])
            condition = list(mask[mask.eq(1)].index)
            bad_df = df.index.isin(condition)
            df_to_complete = df[~bad_df]
            if col in self.flow_meters:
                modified_indices_flow_meters.update(list(df_to_complete[df_to_complete.isna().any(axis = 1)].index))
            else:
                modified_indices_total.update(list(df_to_complete[df_to_complete.isna().any(axis=1)].index))
            df_to_complete[f'mod_{col}'] = df_to_complete[col].interpolate(method = 'linear')
            df = df.join(df_to_complete[[f'mod_{col}']])
            r_data_m = r_data_m.drop([col], axis=1)
            r_data_m[f'{col}'] = df[f'mod_{col}']
        for col in other_col.columns:
            temp_data = other_col[[col]]
            temp_data , modified_indices_total = self.fill_if_previous_equal_next(modified_indices_total, temp_data)
            other_col = other_col.drop([col], axis = 1)
            other_col[col] = temp_data['to_replace']
        data = data.drop(r_data.columns, axis=1)
        data = data.join(other_col)
        data = data.join(r_data_m[r_data.columns.difference(valves)])
        if 'timestampUtc' not in data.columns:
            data['timestampUtc'] = data.index
        data['timestampUtc'] = pd.to_datetime(data['timestampUtc'])
        print(f'org. df : {o_data.isna().sum().sum()} vs. modified df: {data.isna().sum().sum()}, \n'
              f'completing {o_data.isna().sum().sum() - data.isna().sum().sum()} seconds')
        data.loc[list(modified_indices_total),'modified_cell'] = 1
        data.loc[list(modified_indices_flow_meters),'flow_meters_modified'] = 1
        return data

    def fill_categorical_before_after(self, data):
        """
        :param data: takes categorical columns in df
        :return: filled values columns if value before and after null are identical
        """
        modified_indices = set()
        data['modified_categorical_row'] = 0
        data = data.replace(to_replace='', value=np.nan)
        cols = data[self.kpi_categorical].columns[data[self.kpi_categorical].isna().any()].tolist()
        label_encoder = False
        data = data.replace('', pd.NA)
        for i, col in enumerate(cols):
            data2 = data[[col]]
            if data2[col].dtypes == 'object':
                label_encoder = True
                label_enc = LabelEncoder()
                temp = data2.astype("str").apply(label_enc.fit_transform)
                data2 = temp.where(~data2.isna(), data2)
                dictionary_of_labels = dict(zip(label_enc.classes_, label_enc.transform(label_enc.classes_)))
                del dictionary_of_labels['nan']
                dictionary_of_labels = {v: k for k, v in dictionary_of_labels.items()}
            data2[col] = data2[col].astype(float)
            data2[self.datetime_feature_name] = data2.index
            data2['gr'] = np.cumsum((np.abs(data2[col].isna().diff()) > 0) | (np.abs(data2[col].diff()) > 0))
            gr = data2.groupby('gr').first()
            gr['count'] = data2.groupby('gr').count()[self.datetime_feature_name]
            gr['shift_ahead'] = gr[col].shift(-1)
            gr['shift_prev'] = gr[col].shift(1)
            gr['replace'] = (gr[col].isna()) & (gr['shift_ahead'] == gr['shift_prev']) & (
                    gr['count'] <= float(self.time_categorical[i]))
            if 'pc' in self.site_txt or 'cip' in self.site_txt:
                gr['to_replace'] = gr[col]
                gr1 = list(gr[gr['replace'] == 1].index)
                modified_indices.update(list(data2[data2.gr.isin(list(gr.loc[gr['replace'].eq(True)].index))].index))
                print(f'Replacing categorical nans : {self.kpi_categorical}')
                if len(gr1) > 0:
                    gr.loc[gr1, 'to_replace'] = gr.loc[gr1, 'shift_prev']
                    gr.loc[gr['to_replace'].isna(), 'to_replace'] = -1
                    if label_encoder:
                        gr['to_replace'] = gr['to_replace'].apply(lambda x : dictionary_of_labels.get(x))
                    data2 = data2.join(gr['to_replace'], on='gr')
                    data2['to_replace'].ffill(inplace=True)
                    data[col] = data.join(data2['to_replace'])['to_replace']
            else:
                gr.loc[gr['replace'], 'to_replace'] = gr.loc[gr['replace'], 'shift_prev']
                modified_indices.update(list(data2[data2.gr.isin(list(gr.loc[gr['replace'].eq(True)].index))].index))
                gr.loc[gr['to_replace'].isna(), 'to_replace'] = gr.loc[gr['to_replace'].isna(), col]
                data2 = data2.join(gr['to_replace'], on='gr')
                data2['to_replace'].ffill(inplace=True)
                data[col] = data.join(data2['to_replace'])['to_replace']
        data.loc[list(modified_indices),'modified_categorical_row'] = 1
        return data

    def null_statistics(self, old_df, new_df):
        """
        :param old_df: original df without filling nulls
        :param new_df: modified df with nulls filled
        :return: a df with before and after changes for each site
        """
        reference = ['before','after']
        empty_list = []
        over_all_null = []
        empty_total_categorical_fm = []
        for i,df_name in enumerate([old_df,new_df]):
            null_categorical, null_flow_m, under_5, five_to_60, over_60= [0, 0, 0, 0, 0]
            if 'timestampUtc' not in df_name.columns:
                df_name = df_name.reset_index()
            else:
                df_name = df_name.reset_index(drop = True)
            for col in df_name.columns:
                s = df_name[col].isna().groupby(df_name[col].notna().cumsum()).sum()
                s = s[s != 0]
                b = pd.cut(s, bins=[0,5, 60 , 86600], labels=['0-5', '5-60', '6 and above'])
                out = b.groupby(b).size().reset_index(name='Cases').values
                under_5 += out[0,1]
                five_to_60 += out[1,1]
                over_60 += out[2,1]
            empty_list.append([under_5, five_to_60, over_60])
            for col in df_name.columns:
                if col in self.flow_meters:
                    null_flow_m += df_name[col].isnull().sum()
                elif col in self.kpi_categorical:
                    null_categorical += df_name[col].isnull().sum()
            empty_total_categorical_fm.append([null_flow_m, null_categorical])
            tot_null = df_name.isna().sum().sum()
            over_all_null.append(tot_null)
        empty_total_categorical_fm = [a for sub in [[aa]+[bb] for aa, bb in zip(empty_total_categorical_fm[0],empty_total_categorical_fm[1])]  for a in sub]
        columns_not_date = old_df.columns.difference([c for c in old_df.columns if 'date'  in c or 'timestamp' in c or 'local_date' in c]).to_list()
        empty_list = [a for sub in [[aa]+[bb] for aa, bb in zip(empty_list[0],empty_list[1])] for a in sub]
        final_list = [self.site_txt] + [old_df.shape[0]] +[(old_df[columns_not_date].shape[0] - (old_df[columns_not_date].shape[0] - old_df[columns_not_date].dropna().shape[0])) - (new_df[columns_not_date].shape[0] - (new_df[columns_not_date].shape[0] - new_df[columns_not_date].dropna().shape[0]))] + [old_df[columns_not_date].shape[0] - (old_df[columns_not_date].shape[0] - old_df[columns_not_date].dropna().shape[0])] +[new_df[columns_not_date].shape[0] - (new_df[columns_not_date].shape[0] - new_df[columns_not_date].dropna().shape[0])] + over_all_null + [over_all_null[0]/old_df.shape[0]] +[over_all_null[1]/new_df.shape[0]] + empty_total_categorical_fm + empty_list
        over_all_df = pd.DataFrame(final_list).T
        over_all_df.columns  =['site_name','row_recieved' , 'row_fixed', 'full_rows_beforeu','full_rows_after','tot_nulls_before','tot_nulls_after','tot_before %',	'tot_after %','categorical_nulls_before','categorical_nulls_after', 'flow_m_null_before','flow_m_null_after','under_5_sec_before','under_5_sec_after','5s_to_60s_before','5s_to_60s_after','over_60s_before','over_60s_after']
        over_all_df['date']  = pd.to_datetime(new_df[f'{self.datetime_feature_name}']).dt.date[0]
        over_all_df['s3_link'] = f'https://s3.console.aws.amazon.com/s3/buckets/{self.bucket_name}?region=eu-west-1&prefix=ingestion/{self.site_txt}/{list(new_df.index)[0].year}/{list(new_df.index)[0].month}/{list(new_df.index)[0].day}/&showversions=false'
        return over_all_df

    def save_before_after(self, data, old_data):
        if not os.path.exists( os.path.join(self.data_repo, 'ingestion', self.site_txt)):
            os.makedirs(os.path.join(self.data_repo, 'ingestion', self.site_txt))
        data.to_csv(f"{os.path.join(self.data_repo, 'ingestion', self.site_txt)}//modified_df_{pd.to_datetime(data[f'{self.datetime_feature_name}']).dt.date[0]}.csv")
        old_data.to_csv(
            f"{os.path.join(self.data_repo, 'ingestion', self.site_txt)}//original_df_{pd.to_datetime(data[f'{self.datetime_feature_name}']).dt.date[0]}.csv")

    # def resample_data(self,data):
    #     print(f'resampling {self.site_txt} from 1 sec into 1 min')
    #     data = data.apply(pd.to_numeric, downcast='float', errors='ignore')
    #     cols = data.columns
    #     numerical_columns = list(data._get_numeric_data().columns)
    #     categorical_columns = list(set(cols) - set(numerical_columns))
    #     categorical_resample = data[categorical_columns].resample('1min').ffill()
    #     continuous_resample = data[numerical_columns].resample('1min').mean()
    #     data_for_arielle = pd.concat([continuous_resample, categorical_resample], axis = 1)
    #     return data_for_arielle

    def save_data(self, data):
        output_repo_path = os.path.join(self.data_repo, 'ingestion', self.site)
        for name, df in data.groupby(['year', 'month', 'day']):
            for n in name[:-1]:
                output_repo_path = os.path.join(output_repo_path, str(n))
            if not os.path.exists(output_repo_path):
                print(f'path {output_repo_path} doesnt exist. \ncreating path')
                os.makedirs(output_repo_path)
            print(f'ingest data for year {name[0]}, month {name[1]}, day {name[2]} ')
            fn = ''.join([str(c) + '_' for c in name])[:-1] + '.csv'
            final_path = os.path.join(output_repo_path, fn)
            df.drop(columns=['year', 'month', 'day', 'hour'], inplace=True)
            df.to_csv(final_path)
            print('data is saved successfully')

    def main(self):
        invalid_files = pd.DataFrame([])
        df = self.get_data()
        if df.shape[0] > 86401:
            invalid_files = invalid_files.append({'file_name': self.site_txt, 'date': self.start, 'shape of data': df.shape[0]}, ignore_index=True)
        df.index = pd.to_datetime(df.index)
        print('sorting data')
        if 'cip' in self.site_txt:
            df.local_date_time = pd.to_datetime(df.local_date_time)
            df[f'{self.datetime_feature_name}'] = df.local_date_time.copy()
            df[self.filter_column] = df[self.datetime_feature_name].dt.date
            df[self.filter_column] = df[self.datetime_feature_name].dt.date
            df.set_index(self.datetime_feature_name, inplace=True, drop=False)
            df = df.sort_index()
            df = self.sort_and_filter_data(df)
        elif 'tc' in self.site_txt:
            df = self.remove_time_columns_regex(df)
            df = self.sort_and_filter_data(df)
        non_time_features_columns = df[
            df.columns.difference([c for c in df.columns if 'date' in c or 'timestamp' in c])].columns
        df[non_time_features_columns] = df[non_time_features_columns].apply(pd.to_numeric, downcast='float',
                                                                            errors='ignore')
        old_data = df.copy()
        df_new = self.fill_categorical_before_after(df)
        df_new = self.fill_under_n_sec(df_new)
        null_kpi_count_statistics = self.null_statistics(old_data, df_new)
        if self.save_raw_files_locally:
            self.save_before_after(df_new, old_data)
        if self.s3:
            aws_object = AwsS3(self.config, df_new, old_data, self.site_txt, self.datetime_feature_name, self.data_repo)
            key_prefix = aws_object.save_result_onto_s3()
            aws_object.save_pdf_on_s3()
            # aws_object.downloaddirectoryfroms3(key_prefix)
        else:
            visual = Visualize_nulls(df_new, df, self.config)
            visual.plot_by_hour_and_day()
        return null_kpi_count_statistics, invalid_files


if __name__ == '__main__':
    start_date = ['2022-12-02']
    # list_of_sites = ['tarsus_tc1500_line2']
    list_of_sites = ['veurne_tc300_line11', 'murcia_cip_gea', 'murcia_cip_tetra',
                     'tarsus_pc50mz_line1', 'veurne_lopc21_line1', 'veurne_pc42_line4', 'veurne_pc50_line3',
                     'zb_cip_blending', 'zb_cip_tankfarm']
    starts = start_date * len(list_of_sites)
    data_repo = r'C:\Users\HadarHolan\data_repo'
    tot_nulls_before_after = pd.DataFrame([])
    invalid_files_days = pd.DataFrame([])
    end_date = '2022-12-02'
    for j, site in enumerate(list_of_sites):
        config_fp = f'C:\\Users\\hadarh\\OneDrive - Aqua Rimat\\Documents\\PycharmProjects\\tc_ml_pipeline\\' \
                    f'site_and_email_config_files\\{site}_config.txt'
        config_dict = io_utils.parse_config_dictionary_file(config_fp)
        dates = pd.date_range(starts[j], end_date, freq='d')
        for d in dates:
            es_s3 = ES_to_S3(site, d, d + timedelta(seconds=86400 - 1), 'data_repo//tc_ml_pipeline', config_dict, True)
            try:
                null_statistics, invalid_input = es_s3.main()
                tot_nulls_before_after = pd.concat([tot_nulls_before_after, null_statistics])
                invalid_files_days = pd.concat([invalid_files_days, invalid_input])
            except Exception as ex:
                print(f'Day {d} was not ingested. Error is: {ex}')

    deployment_config_path = f'C:\\Users\\hadarh\\OneDrive - Aqua Rimat\\Documents\\PycharmProjects\\' \
                             f'tc_ml_pipeline\\tc-predictor-service\\deploy_config.txt'
    config_deploy = io_utils.parse_config_dictionary_file(deployment_config_path)
    topic = 'null report per site before and after'
    ending = datetime.strptime(end_date, "%Y-%m-%d").date()
    email_service = EMailService(config_deploy['email']['sender'], config_deploy['email']['recipients'], topic, ending)
    text = email_service.adjust_text()
    email_service.send_email(text, tot_nulls_before_after)
