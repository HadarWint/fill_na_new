import os
import argparse
from typing import Mapping
from all_sites_na_completion import ES_to_S3
from datetime import datetime
import io_utils
import pandas as pd
from datetime import timedelta
from email_service import EMailService

class DataSource:
    @staticmethod
    def local():
        return 'local'

    @staticmethod
    def s3():
        return 's3'


# class Try(argparse.ArgumentParser):
#     def __init__(self):
#         # todo to avoid Unresolved attribute reference 'data_source' for class 'ArgumentParser'
#         super().__init__()

class LauncherArgs:
    def __init__(self,
                 start: str,
                 end: str,
                 data_repo: str,
                 config_file_path: str,
                 save_onto_s3: str,
                 save_locally_to_pc: str,
                 list_of_sites: list,
                 deployment_config_path: str,
                 offline=False):
        self.start: str = start
        self.end: str = end
        self.data_repo: str = data_repo
        self.config: str = config_file_path
        self.save_onto_s3: str = save_onto_s3
        self.save_locally_to_pc: str = save_locally_to_pc
        self.list_of_sites: list = list_of_sites
        self.deployment_config_path = deployment_config_path
        self.offline = offline


class CmdlineLauncher:
    def __init__(self):
        pass

    @staticmethod
    def load_config(args: Mapping[str, object],site_name):
        config_path = os.path.join(args.config_file_path, f'{site_name}_config.txt')
        config_file = io_utils.parse_config_dictionary_file(config_path)
        if bool(config_file):
            print('Config file loaded successfully')
        else:
            print('ERROR: config file is empty')
            raise Exception(f'Config file read failed from path = {config_path} file is empty')
        return config_file

    @staticmethod
    def run_job(self, args):
        print('#########Launch TC anomaly detection #########')
        print(f'Running report with the following args: = {args}\n')
        tot_nulls_before_after = pd.DataFrame([])
        invalid_files_days = pd.DataFrame([])
        entities = None
        starts = [args.start] * len(args.list_of_sites)
        for j, site in enumerate(args.list_of_sites):
            dates = pd.date_range(starts[j], args.end, freq='d')
            for d in dates:
                config_dict = self.load_config(args, site)
                fill_na = ES_to_S3(site, d, d + timedelta(seconds=86400 - 1),args.data_repo , config_dict, True, True,
                                   True)
                null_statistics, invalid_input = fill_na.run()
                tot_nulls_before_after = pd.concat([tot_nulls_before_after, null_statistics])
                invalid_files_days = pd.concat([invalid_files_days, invalid_input])
        return tot_nulls_before_after


    def launch(self):
        args = self.parse_cmd_line_parameters()
        tot_nulls_before_after = self.run_job(self, args)
        config_deploy = self.load_config(args, 'deploy')
        topic = 'null report per site before and after'
        ending = datetime.strptime(args.end, "%Y-%m-%d").date()
        email_service = EMailService(config_deploy['email']['sender'], config_deploy['email']['recipients'], topic,
                                     ending)
        text = email_service.adjust_text()
        email_service.send_email(text, tot_nulls_before_after)

    @staticmethod


    def parse_cmd_line_parameters() -> Mapping[str, object]:
        print(f'Parsing parameters')
        true_str = str(True)
        false_str = str(False)

        parser = argparse.ArgumentParser(description='Basic boilerplate, ignore the details.')
        parser.add_argument('--start', dest='start', default=os.environ.get('start'))
        parser.add_argument('--end', dest='end', default=os.environ.get('end'))
        parser.add_argument('--data_repo', dest='data_repo', default= os.environ.get('data_repo'))
        parser.add_argument('--config_file_path', dest='config_file_path', default= os.environ.get('config_file_path'))
        parser.add_argument('--save_onto_s3', dest='save_onto_s3', choices=[true_str, false_str], default=true_str)
        parser.add_argument('--save_locally_to_pc', dest='save_locally_to_pc', choices=[true_str, false_str], default=true_str)
        parser.add_argument('--list_of_sites', dest='list_of_sites', default=os.environ.get('list_of_sites').split(','))

        parser.add_argument('--deployment_config_path', dest='deployment_config_path', default=os.environ.get('deployment_config_path'))

        # back doors:
        parser.add_argument('--offline', dest='offline',  choices=[true_str, false_str], default=true_str)

        args = parser.parse_args()
        args.data_repo = os.path.expandvars(args.data_repo)

        args.save_onto_s3 = True if args.save_onto_s3 == 'True' else False
        args.end = datetime.utcnow() if args.end == 'now' else datetime.strptime(
            str(args.end), '%Y-%m-%d')
        args.start = datetime.strptime(str(args.start), '%Y-%m-%d')
        print(f'Argument for this job are: {args}')
        return args


if __name__ == '__main__':
    cli_launcher = CmdlineLauncher()
    cli_launcher.launch()