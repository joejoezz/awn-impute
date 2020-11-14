"""
pull the latest metadata, compare to existing archives of obs/estimators/predictions
if a station doesn't exist in metadata, erase all data for it
saves a metadata file of station status
run this before re-training estimators
"""

import sys
import os
import time
sys.path.append('/data/awn/AWNPy')
from AWNPy import AWN, AWNPyError
import pdb
import pandas as pd
from datetime import datetime, timedelta
from util import extract_stids
from glob import glob

def main(config):
    """
    main downloding function
    :param config:
    :return: config file
    """

    root_dir = config['ROOT_DIR']
    obs_dir = config['OBS_ROOT']#'/data/awn/impute/oper/obs'
    pre_dir = config['PREDICTIONS_ROOT']#'/data/awn/impute/oper/predictions'
    est_dir = config['ESTIMATORS_ROOT']#

    download_start_date = pd.to_datetime(config['data_start_date']) #2012-01-01
    vars = config['vars']

    now = datetime.utcnow() - timedelta(hours=7)
    # station must have been installed after this date to be counted
    install_thres = pd.to_datetime(datetime.utcnow() - timedelta(days=365))

    m = AWN(username=config['awn_api_username'], password=config['awn_api_password'])
    metadata = m.metadata(return_dataframe=True)
    metadata['INSTALLATION_DATE'] = pd.to_datetime(metadata['INSTALLATION_DATE'])
    tier1 = metadata[metadata['TIER'] == '1']
    tier2 = metadata[metadata['TIER'] == '2']

    # trim based on install thresh
    tier1 = tier1[tier1['INSTALLATION_DATE'] < install_thres]
    tier2 = tier2[tier2['INSTALLATION_DATE'] < install_thres]
    tier1 = tier1[tier1['STATION_VISIBILITY'] == 'public']
    tier2 = tier2[tier2['STATION_VISIBILITY'] == 'public']
    all_stns = pd.concat((tier1, tier2))

    df_metadata = pd.DataFrame(index=all_stns['STATION_ID'], columns=['name', 'lat', 'lon', 'installation_date', 'tier'])
    df_metadata['name'] = all_stns['STATION_NAME'].values
    df_metadata['lat'] = all_stns['LATITUDE_DEGREE'].astype('float').values
    df_metadata['lon'] = all_stns['LONGITUDE_DEGREE'].astype('float').values
    df_metadata['installation_date'] = all_stns['INSTALLATION_DATE'].values
    df_metadata['tier'] = all_stns['TIER'].values

    df_metadata.to_csv('{}/metadata/stn_metadata.csv'.format(root_dir))

    # go through each set of files and see if station IDs match metadata. If not, delete.
    meta_stids = df_metadata.index.values

    obs_files = glob('{}/*'.format(obs_dir))
    stids_obs = extract_stids(obs_files)

    est_files = glob('{}/*'.format(est_dir))
    stids_est = extract_stids(est_files)

    pre_files = glob('{}/*'.format(pre_dir))
    stids_pre = extract_stids(pre_files)

    # loop through STIDS, check if exists, if not, download from download start date (specified in config file),
    #  otherwise download from most recent date
    for stid in stids_obs:
        if stid not in meta_stids:
            obs_file = '{}/obs_{}.p'.format(obs_dir, stid)
            os.remove(obs_file)
            print('Removed {}'.format(obs_file))

    for stid in stids_est:
        if stid not in meta_stids:
            est_file = '{}/estimator_{}.joblib'.format(est_dir, stid)
            os.remove(est_file)
            print('Removed {}'.format(est_file))

    for stid in stids_pre:
        if stid not in meta_stids:
            pre_file = '{}/predictions_{}.p'.format(pre_dir, stid)
            os.remove(pre_file)
            print('Removed {}'.format(pre_file))

    return
