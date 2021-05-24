""" Definition Aggregator """
import os
import importlib
import glob
import zipfile
import json
from typing import List
from accounts.accounts import fetch_account_streamers
 
class Definition():
    """ Aggregated Definitions """
    metric_sets: List
    sla_sets: List
    def __init__(self, account):
 
        self.metric_sets: List = []
        self.sla_sets: List = []
        self.account_definitions = []
        try:
            trying = os.listdir(os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                f'account_{account}'
            ))
            dir_path = os.path.join(
                os.path.dirname(
                    os.path.realpath(__file__)),f'account_{account}', '**/*'
                )
            self.iterate_definitions(dir_path)
        except NotADirectoryError:
            with zipfile.ZipFile('/tmp/definitions.zip', 'r') as zip_ref:
                zip_ref.extractall('/tmp')
            dir_path = os.path.join(
                f'/tmp/definitions/account_{account}', '**/*'
                )
            self.iterate_definitions(dir_path)

    def iterate_definitions(self, dir_path):
        """ Iterate through the modules. """
        for filename in glob.iglob(dir_path, recursive=True):
            if os.path.isfile(filename):
                if filename.endswith('__init__.py') or not filename.endswith('.py'):
                    continue
                self.account_definitions.append(filename)
        self.generate_sla_metrics()

    def generate_sla_metrics(self):
        """ Generate SLAs and metrics. """
        for module in self.account_definitions:

            metric_spec = Definition.return_spec(
                type_set='metric_set',
                module=module
            )
 
            metric_module = importlib.util.module_from_spec(metric_spec)
            metric_spec.loader.exec_module(metric_module)
            try:
                self.metric_sets.append(metric_module.metric_set)
            except AttributeError as _ex:
                print("Module has no attribute metric_set")
            sla_spec = Definition.return_spec(
                type_set='sla_set',
                module=module
            )
 
            sla_module = importlib.util.module_from_spec(sla_spec)
            sla_spec.loader.exec_module(sla_module)
            try:
                self.sla_sets.append(sla_module.sla_set)
            except AttributeError as _ex:
                print("Module has no attribute sla_set")
                
    @staticmethod
    def return_spec(type_set, module):
        """ Static Method to return the file spec. """
        spec = importlib.util.spec_from_file_location(
            type_set,
            module
        )
        return spec
 
class DefinitionSet():
    """ Definitions for entire cdk app stage"""
    metric_sets: List
    sla_sets: List
    def __init__(self, account):
        self.metric_sets: List = []
        self.sla_sets: List = []
        
        accounts = fetch_account_streamers(account)
        for acc in accounts:
            defenition = Definition(account=acc)
            for metric_set in defenition.metric_sets:
                for metric in metric_set.metrics:
                    metric_details = metric.__dict__
                    metric_details['account'] = acc
                    if metric_details['metadata']:
                        metadata_map = {}
                        for meta in metric_details['metadata']:
                            metadata_map[meta.name] = meta.value
                        metric_details['metadata'] = json.dumps(metadata_map)
                    if metric_details['dimensions']:
                        dimensions_map = {}
                        for dimension in metric_details['dimensions']:
                            dimensions_map[dimension.name] = dimension.value
                        metric_details['dimensions'] = json.dumps(dimensions_map)
                    metric_details['metric_set'] = metric_details['metric_set'].name
                    metric_details['dashboard'] = metric_details['dashboard'].dashboard_name
                    if 'dataset' in metric_details:
                        metric_details['dataset'] = json.dumps(metric_details['dataset'].__dict__)
                    if 'analyzers' in metric_details:
                        metric_details['analyzers'] = json.dumps(metric_details['analyzers'])
                    if 'checkers' in metric_details:
                        metric_details['checkers'] = json.dumps(metric_details['checkers'])
                    if 'reference_datasets' in metric_details:
                        metric_details['reference_datasets'] = json.dumps(metric_details['reference_datasets'])
                    self.metric_sets.append(metric_details)
            for sla_set in defenition.sla_sets:
                for sla in sla_set.slas:
                    sla_details = sla.__dict__
                    del sla_details['sla_set']
                    sla_details['metric_namespace'] = sla_details['metric'].namespace
                    sla_details['metric_name'] = sla_details['metric'].name
                    sla_details['metric_set'] = sla_details['metric'].metric_set.name
                    if sla_details['metric'].metadata:
                        metadata_map = {}
                        for meta in sla_details['metric'].metadata:
                            metadata_map[meta.name] = meta.value
                        sla_details['metric_metadata'] = json.dumps(metadata_map)
                    if sla_details['metric'].dimensions:
                        dimensions_map = {}
                        for dimension in sla_details['metric'].dimensions:
                            dimensions_map[dimension.name] = dimension.value
                        sla_details['metric_dimensions'] = json.dumps(dimensions_map)
                    del sla_details['metric']
                    sla_details['account'] = acc
                    self.sla_sets.append(sla_details)
