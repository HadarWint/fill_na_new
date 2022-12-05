import datetime
import io
import os
import tempfile
from pathlib import PurePath
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import missingno as msno
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages


class AwsS3:
    def __init__(self, config, ouput_file, old_data, site_name, date_time, data_repo, key_prefix: str = ''):
        try:
            aws_profile = os.getenv('PEPSICO_PREDICTOR_SVC_AWS_PROFILE')
            if aws_profile is not None:
                session = boto3.Session(profile_name=aws_profile)
            else:
                session = boto3.Session()
            self.s3 = session.resource('s3')
            self.s3_client = boto3.client('s3')
        except ClientError as ex:
            print(f'S3 client error - {ex}')
        self.kpi_continous = config['pre_p']['flow_meter_na_completion']
        self.kpi_categorical = config['pre_p']['categorical_feature']
        self.kpi_columns = self.kpi_continous + self.kpi_categorical
        self.key_prefix = key_prefix
        self.temp_dir = tempfile.TemporaryDirectory(prefix='aws_s3')
        self.temp_path = PurePath(self.temp_dir.name)
        self.df_to_s3 = ouput_file
        self.bucket = config['s3']['op_bucket']
        self.directory = config['s3']['directory_path']
        self.old_df = old_data
        self.datetime_feature = date_time
        self.site_name = site_name
        save_locally_afterwards = True
        if save_locally_afterwards:
            self.local_s3_output = os.path.join(data_repo, 's3')

    #
    # def __del__(self):
    #     self.temp_dir.cleanup()
    #
    # def algo_bucket(self) -> Bucket:
    #     return self.s3.Bucket('tc-ml-pipeline')
    #
    # def put_object(self, key: str, data: Union[bytes, IO[bytes], StreamingBody]) -> Union[Object, None]:
    #     try:
    #         return self.algo_bucket().put_object(Key=key, Body=data)
    #     except ClientError as ex:
    #         print(f'S3 upload error for key: {key} - {ex}')
    #         return None

    # def put_keyed_file(self, os_path: str, key: str, callback: Callable[..., Any] = None) -> None:
    #     try:
    #         self.algo_bucket().upload_file(Filename=os_path, Key=key, Callback=callback)
    #     except (ClientError, BaseException) as ex:
    #         print(f'S3 upload error for key: {key} - {ex}')
    #
    # def read_keyed_file(self, key: str, dest: str, callback: Callable[..., Any] = None) -> None:
    #     """
    #     :param key: The S3 object key.
    #     :type dest: The local file-system destination path.
    #     :param callback: Callback for getting progress updates.
    #     """
    #     self.algo_bucket().download_file(Key=key, Filename=dest, ExtraArgs=None, Callback=callback, Config=None)

    # def read_keyed_file_obj(self, key: str, filename: str, callback: Callable[..., Any] = None) -> [IO[Any], None]:
    #     tmp_file = tempfile.NamedTemporaryFile(suffix='_' + filename, dir=self.temp_path, mode='w+b', delete=False)
    #     if tmp_file is not None:
    #         try:
    #             self.algo_bucket().download_fileobj(Key=key, Fileobj=tmp_file.file, Callback=callback, Config=None)
    #             tmp_file.flush()
    #             tmp_file.close()
    #             return tmp_file
    #         except ClientError as ex:
    #             print(f'S3 download error for key: {key} - {ex}')
    #             return None
    #     else:
    #         return None

    # def read_file_obj(self, os_path: PurePath, callback: Callable[..., Any] = None) -> [IO[Any], None]:
    #     return self.read_keyed_file_obj(filename=os_path.name, key=self.path_to_key(os_path), callback=callback)
    #
    def path_to_key(self, path: PurePath) -> str:
        """
        Make sure a filesystem OS path is formatted correctly for being used as an S3 key
        :param path: A file system path
        :return: A potentially reformatted key
        """
        return self.prefix_key(path.as_posix())

    def prefix_key(self, key: str) -> str:
        return self.key_prefix + key

    def downloaddirectoryfroms3(self, key_prefix):
        """
        :param key_prefix: the local key in S3
        :return: files saved locally on the local_s3_output path
        """
        bucket = self.s3.Bucket(self.bucket)
        try:
            for obj in bucket.objects.filter(Prefix=key_prefix):
                if not os.path.exists(os.path.dirname(os.path.join(self.local_s3_output,obj.key))):
                    os.makedirs(os.path.dirname(os.path.join(self.local_s3_output,obj.key)))
                bucket.download_file(obj.key, os.path.join(self.local_s3_output,obj.key))
            print(f'download to local pc in {self.local_s3_output}')
        except Exception as ex:
            print(f'file was not downl. Error is: {ex}')

    def save_result_onto_s3(self):
        df = self.df_to_s3
        df['hour'] = pd.to_datetime(df[f'{self.datetime_feature}']).dt.hour
        if isinstance(list(df.index)[0], str):
            year = datetime.datetime.strptime(list(df.index)[0], "%Y-%m-%dT%H:%M:%S").year
            month = datetime.datetime.strptime(list(df.index)[0], "%Y-%m-%dT%H:%M:%S").month
            day = datetime.datetime.strptime(list(df.index)[0], "%Y-%m-%dT%H:%M:%S").day
        else:
            year = list(df.index)[0].year
            month = list(df.index)[0].month
            day = list(df.index)[0].day
        key = self.directory + '/' + self.site_name + '/' + str(year) + '/' + str(month) + '/' + str(day)
        for hour, hour_df in df.groupby('hour'):
            final_key = key + '/' + str(hour)
            try:
                self.s3.Object(self.bucket, f'{final_key}/file.csv').put(Body=hour_df.to_csv())
            except Exception as ex:
                print(f'pdf for hour {hour} was not uploaded. Error is: {ex}')
        print(f'date {year}-{month}-{day} was successfully uploaded!')
        return key

    def plot_kpi_columns_s3(self, date, i, pdf, synthesized_df):
        fig = msno.matrix(synthesized_df[self.kpi_columns], freq='H', figsize=(15, 10), fontsize=4, sparkline=False,
                          p=1)
        if synthesized_df.shape[0] <= 86400:
            fig.set_yticklabels(np.arange(0, 24))
        else:
            fig.set_yticklabels(np.arange(0, 25))
        fig.set_xlabel(f'kpis - {date}-{i}, nulls:'
                       f'{synthesized_df[self.kpi_categorical].isna().sum().sum()}'
                       f'categorical, {synthesized_df[self.kpi_continous].isna().sum().sum()}'
                       f'in flow meters')
        pdf.savefig()

    def plot_all_columns(self, date, i, pdf, synthesized_df):
        fig = msno.matrix(synthesized_df, freq='H', figsize=(15, 10), fontsize=4,
                          sparkline=False, p=1)
        if synthesized_df.shape[0] <= 86400:
            fig.set_yticklabels(np.arange(0, 24))
        else:
            fig.set_yticklabels(np.arange(0, 25))
        fig.set_xlabel(f'{date}-{i}, nulls:'
                       f' {synthesized_df[self.kpi_categorical].isna().sum().sum()} categorical'
                       f', {synthesized_df[self.kpi_continous].isna().sum().sum()} in flow meters '
                       f'and {synthesized_df.isna().sum().sum()} total')
        pdf.savefig()
        return pdf

    def save_pdf_on_s3(self):
        df = self.df_to_s3.copy()
        if isinstance(list(df.index)[0], str):
            year = datetime.datetime.strptime(list(df.index)[0], "%Y-%m-%dT%H:%M:%S").year
            month = datetime.datetime.strptime(list(df.index)[0], "%Y-%m-%dT%H:%M:%S").month
            day = datetime.datetime.strptime(list(df.index)[0], "%Y-%m-%dT%H:%M:%S").day
        else:
            year = list(df.index)[0].year
            month = list(df.index)[0].month
            day = list(df.index)[0].day
        key = self.directory + '/'+self.site_name + '/' + str(year) + '/' + str(month) + '/' + str(day)
        with io.BytesIO() as output:
            with PdfPages(output) as pdf:
                for i, df_sample in enumerate([self.old_df,df ]):
                    if 'timestampUtc' in df_sample.columns:
                        df_sample = df_sample.reset_index(drop=True)
                    else:
                        df_sample = df_sample.reset_index()
                    new_df = df_sample.copy().reset_index()
                    date = pd.to_datetime(new_df[f'{self.datetime_feature}']).dt.date.unique()[0]
                    new_df = new_df.set_index(pd.to_datetime(new_df[f'{self.datetime_feature}']).dt.round('1S'))
                    new_df[self.datetime_feature] = pd.to_datetime(new_df[f'{self.datetime_feature}'])
                    interesting_features_df = new_df[
                        new_df.columns.difference([c for c in new_df.columns if 'date' in c or 'timestamp' in c])]
                    if new_df.shape[0] < 86400:
                        temp = pd.DataFrame(pd.date_range(start=date, freq='1s', periods=86400),columns=['time'])
                        temp = temp.set_index('time')
                        synthesized_df = temp.join(new_df)
                        # saving all columns
                        self.plot_all_columns(date, i, pdf, synthesized_df)
                        # saving only kpi columns
                        self.plot_kpi_columns_s3(date, i, pdf, synthesized_df)
                    else:
                        self.plot_all_columns(date, i, pdf, interesting_features_df)
                        self.plot_kpi_columns_s3(date, i, pdf, interesting_features_df)
            data = output.getvalue()
            file = key + '/' + 'before_and_after.pdf'
            try:
                self.s3.Object(self.bucket, f'{file}').put(Body=data)
                print(f'uploaded successfuly onto {key} in bucket {self.bucket}')
            except Exception as ex:
                print(f'pdf  was not uploaded. Error is: {ex}')

