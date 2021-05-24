""" Fetches Accounts """

import json
import zipfile

try:
    with open('accounts/config.json') as f:
        accounts = json.load(f)
except FileNotFoundError:
    with zipfile.ZipFile('/tmp/accounts.zip', 'r') as zip_ref:
        zip_ref.open('accounts/config.json')


def fetch_account_central(account:str):
    """ Returns central account number for a given account from accounts config. """
    for config in accounts:
        if account in config['streamers']:
            return config['central']
    return

def fetch_account_streamers(account:str):
    """ Returns streamer accounts for a given account from accounts config. """
    for config in accounts:
        if account in config['streamers']:
            return config['streamers']
    return

def fetch_account_catalogs(account:str):
    """ Returns catalog accounts for a given account from accounts config. """
    for config in accounts:
        if account in config['streamers']:
            return config['catalogs']
    return
