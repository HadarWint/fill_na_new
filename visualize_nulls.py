import pandas as pd
import numpy as np
import missingno as msno
import os
import datetime
from matplotlib.backends.backend_pdf import PdfPages

class Visualize_nulls:
    def __init__(self, data,old_data,config  ):
        self.kpi_continuos = config['pre_p']['flow_meter_na_completion']
        self.kpi_categorial = config['pre_p']['categorical_feature']
        self.kpi_columns = self.kpi_continuos + self.kpi_categorial
        self.df = data[self.kpi_columns]
        self.old_df = old_data[self.kpi_columns]
        self.datetime_feature = config['general']['datetime_feature']
        self.site = config['general']['site_prefix']
        self.op_p = config['visual_nulls']['path']
        self.output_path = os.path.join(self.op_p, self.site)
        self.save_locally = False



    def plot_by_hour_and_day(self):
        try:
            year = datetime.datetime.strptime(list(self.df.index)[0], "%Y-%m-%dT%H:%M:%S").year
            month = datetime.datetime.strptime(list(self.df.index)[0], "%Y-%m-%dT%H:%M:%S").month
            day = datetime.datetime.strptime(list(self.df.index)[0], "%Y-%m-%dT%H:%M:%S").day
        except:
            year = datetime.datetime.strptime(list(self.df.index)[0], "%Y-%m-%d %H:%M:%S").year
            month = datetime.datetime.strptime(list(self.df.index)[0], "%Y-%m-%d %H:%M:%S").month
            day = datetime.datetime.strptime(list(self.df.index)[0], "%Y-%m-%d %H:%M:%S").day
        position  = ['before','after']
        if not os.path.exists(f'{self.op_p }\\{year}\\{month}\\{day}'):
            os.makedirs(f'{self.op_p }\\{year}\\{month}\\{day}')
        with PdfPages(f'{self.op_p }\\{year}\\{month}\\{day}\\before_after.pdf') as pdf:
            for i,df in enumerate([self.old_df, self.df ]):
                new_df = df.copy().reset_index()
                new_df.columns  =  [self.datetime_feature] + self.kpi_columns
                date = pd.to_datetime(new_df[f'{self.datetime_feature}']).dt.date.unique()[0]
                new_df = new_df.set_index(pd.to_datetime(new_df[f'{self.datetime_feature}']))
                new_df[self.datetime_feature] = pd.to_datetime(new_df[f'{self.datetime_feature}'])
                # new_df =new_df.drop([f'{self.datetime_feature}'],axis = 1)
                if new_df.shape[0] < 86400:
                    synthesized_df = pd.DataFrame(pd.date_range(start=new_df.index.min(), freq='1s', periods=86400),
                   columns=['time'])
                    synthesized_df = synthesized_df.set_index('time')
                    synthesized_df = synthesized_df.join(new_df)
                    #
                    synthesized_df.columns = [f'{self.datetime_feature}'] + self.kpi_columns
                    synthesized_df = synthesized_df.reset_index()
                    condition = new_df.index.max()
                    where_to_replace = synthesized_df['time'] > condition
                    synthesized_df.at[where_to_replace, 1:] = 1
                    synthesized_df = synthesized_df.set_index('time')
                    fig = msno.matrix(synthesized_df, freq='H', figsize=(15, 10), fontsize=6, sparkline=False, p=1)
                    fig.set_yticklabels(np.arange(0, 24))
                    fig.set_xlabel(f'{date}-{position[i]}')
                    try:
                        pdf.savefig(fig.figure)
                    except Exception as ex:
                        print(ex)
                else:
                    fig = msno.matrix(new_df, freq='H', figsize=(15, 10), fontsize=6, sparkline=False, p=1)
                    fig.set_yticklabels(np.arange(0, 24))
                    fig.set_xlabel(f'{date}-{position[i]}')
                    try:
                        pdf.savefig(fig.figure)
                    except Exception as ex:
                        print(ex)
            pdf.close()
            print(f'pdf saved successfully onto {self.output_path}')


