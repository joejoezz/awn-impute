"""
QAQC algorithm
Writes an entirely new QAQC CSV file every time for each station
Also writes a summary QAQC CSV file
"""

import pdb
from glob import glob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
import re
from util import extract_stids


def qaqc_one_station(df_obs, df_predict, stid, vars, flag_thresholds, now):
    """
    run QAQC
    :param df_obs: obs df
    :param df_predict: predictions df
    :param stid: station ID
    :param vars: variables we are QCing
    :prarm flag_thresholds: QAQC thresholds
    :param now: current time
    :return:
    """

    # create our columns:
    qaqc_columns = []
    for var in vars:
        qaqc_columns.append('{}_IMPUTE'.format(var))
        qaqc_columns.append('{}_DIFFERENCE'.format(var))
        qaqc_columns.append('{}_ROLLING_DIFFERENCE'.format(var))
        qaqc_columns.append('{}_FLAG'.format(var))

    # create our QAQC database -- predictions ultimately constrain this
    df_qaqc = pd.DataFrame(index=df_predict.index, columns=qaqc_columns)

    # populate dataframe
    for var in vars:
        difference = (df_predict[var] - df_obs[var]).dropna()
        df_qaqc['{}_DIFFERENCE'.format(var)] = difference
        df_qaqc['{}_IMPUTE'.format(var)] = df_predict[var]

    # compute running mean of difference to generate QAQC flag
    for i, var in enumerate(vars):
        difference = df_qaqc['{}_DIFFERENCE'.format(var)]
        rolling_diff = difference.rolling(196).mean()
        df_qaqc['{}_ROLLING_DIFFERENCE'.format(var)] = rolling_diff
        flag = rolling_diff.copy()
        flag[abs(rolling_diff) < flag_thresholds[i]] = 0
        flag[abs(rolling_diff) >= flag_thresholds[i]] = 1
        df_qaqc['{}_FLAG'.format(var)] = flag
        flags_last_day = len(np.where((flag[flag.index > now-timedelta(days=1)] == 1))[0])
        if len(rolling_diff.dropna()) > 0:
            print('{} QAQC {} flags last day: {}'.format(stid, var, flags_last_day))
            print('{} QAQC {} latest rolling mean: {}'.format(stid, var, np.around(rolling_diff.dropna()[-1],1)))
        else:
            print('! Warning ! No QA flags computed for {} {}'.format(stid, var))

    return df_qaqc


def main(config):
    """
    main prediction code
    :return:
    """
    predict_dir = config['PREDICTIONS_ROOT']#'/data/awn/impute/oper/predictions'
    obs_dir = config['OBS_ROOT']  # '/data/awn/impute/oper/obs'
    qaqc_dir = config['QAQC_ROOT']  # '/data/awn/impute/oper/qaqc'
    predict_start_date = pd.to_datetime(config['predict_start_date']) #2020-11-10
    vars = config['vars']
    flag_thresholds = [float(i) for i in config['flag_thresholds']]

    now = datetime.utcnow() - timedelta(hours=7)

    # pull the list of STNIDs from obs and predictions directories
    # we only run the QAQC checks if we have both obs and predictions
    pre_dir = config['PREDICTIONS_ROOT']
    pre_files = glob('{}/*'.format(pre_dir))
    stids_pre = extract_stids(pre_files)
    obs_files = glob('{}/*'.format(obs_dir))
    stids_obs = extract_stids(obs_files)

    # the line below only works in Python 3.5 or newer apparently
    stids = [*(set(stids_obs) & set(stids_pre))]

    print('QAQC for {} stations'.format(len(stids)))
    # loop through STIDS and call the QAQC function
    for stid in stids:
        predict_file = '{}/predictions_{}.p'.format(predict_dir, stid)
        obs_file = '{}/obs_{}.p'.format(obs_dir, stid)

        df_obs = pd.read_pickle(obs_file)
        df_predict = pd.read_pickle(predict_file)
        df_qaqc = qaqc_one_station(df_obs, df_predict, stid, vars, flag_thresholds, now)

        df_qaqc.to_csv('{}/qaqc_{}.csv'.format(qaqc_dir, stid))
        print('saved {}'.format(stid))

    return