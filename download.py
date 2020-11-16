"""
download raw obs -- tier 1 and tier 2 only, must have been installed at least 1 year ago
1) load existing pandas df of obs
2) find last obs
3) download to fill in up to most recent
4) eventually this will run every 15 minutes
"""

import sys
import os
import time
sys.path.append('/data/awn/AWNPy')
from AWNPy import AWN, AWNPyError
import pdb
import pandas as pd
from datetime import datetime, timedelta

def obs_call(m, stid, start, end):
    sd = m.stationdata(STATION_ID=stid, START=start, END=end, return_dataframe=True)
    return sd


def download_one_station(m, stid, vars, start_time, end_time):
    start = start_time
    df_tmp = pd.DataFrame()
    while start < end_time:
        end_tmp = start + timedelta(days=99, hours=23, minutes=45)
        try:
            sd = obs_call(m, stid, start, end_tmp)
        except:
            print('fail')
            time.sleep(15)
            try:
                sd = obs_call(stid, start, end_tmp)
            except:
                print('fail2')
                time.sleep(15)
                try:
                    sd = obs_call(stid, start, end_tmp)
                except:
                    print('API ERROR for data from {} to {} (it might not exist)'.format(start, end_tmp))
                    sd = pd.DataFrame()
        if sd.empty is False:
            df_tmp = pd.concat((df_tmp, sd[vars]))
        start += timedelta(days=100)
    return df_tmp

def main(config):
    """
    main downloding function
    :param config:
    :return: config file
    """

    obs_dir = config['OBS_ROOT']#'/data/awn/impute/oper/obs'
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

    print('Getting data for {} tier-1 and {} tier-2 stations'.format(len(tier1), len(tier2)))

    # loop through STIDS, check if exists, if not, download from download start date (specified in config file),
    #  otherwise download from most recent date
    for stid in all_stns['STATION_ID']:
        meta_stid = all_stns[all_stns['STATION_ID'] == stid]
        obs_file = '{}/obs_{}.p'.format(obs_dir, stid)
        if os.path.isfile(obs_file):
            print('found existing obs for {}'.format(stid))
            df = pd.read_pickle(obs_file)
            last_obs_date = df.index[-1]
            df_latest = download_one_station(m, stid, vars, last_obs_date+timedelta(minutes=15), now)
            if df_latest.empty is False:
                df = pd.concat((df, df_latest))
                print('found more obs from {} to {}'.format(df_latest.index[0], df_latest.index[-1]))
            else:
                print('no new obs found')
            print('latest obs: {}'.format(df.index[-1]))
            print('saved {}'.format(stid))
        else:
            print('no existing obs for {}'.format(stid))
            install_date = meta_stid['INSTALLATION_DATE']
            if install_date.values[0] < download_start_date:
                install_date = download_start_date
            else:
                install_date = pd.to_datetime(install_date.values[0])
            print('downloading from {}'.format(install_date))
            df = download_one_station(m, stid, vars, install_date, now)
            # save data
            if not df.empty:
                print('latest obs: {}'.format(df.index[-1]))
            print('saved {}'.format(stid))
        df.to_pickle(obs_file)

