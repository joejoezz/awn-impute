"""
make predictions
"""

import pdb
import re
from glob import glob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
from joblib import load
from sklearn.impute import SimpleImputer
from util import extract_stids


def predict_one_station(df_obs, feature_columns, est_dir, stid, vars, start_time, end_time):
    """
    make predictions for one station
    :param df_obs: dataframe of all obs
    :param feature_columns: features used for this estimator
    :param est_dir: directory of the estimator
    :param stid: station ID
    :param vars: variables to predict
    :param start_time: start time of predictions
    :param end_time: end time of predictions
    :return: dataframe of predictions
    """
    # Create X -- first trim to only feature columns
    X = df_obs[feature_columns]

    # drop the columns of this station from X and create columns to count features
    columns_tmp = []
    columns_out = []
    for var in vars:
        columns_tmp.append('{}_{}'.format(stid, var))
        columns_out.append('{}'.format(var))
    columns_out.append('NA_COUNT')
    X = X.drop(columns=columns_tmp)

    # drop nas
    X = X.dropna(how='all')
    na_count = X.isna().sum(axis=1)

    # impute obs
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    X_simple = imp_mean.fit_transform(X)

    # trim X_simple times:
    X_simple = X_simple[(X.index >= start_time) & (X.index <= end_time)]

    clf = load('{}/estimator_{}.joblib'.format(est_dir, stid))
    y = clf.predict(X_simple)
    y = np.around(y,1)

    df_predict = pd.DataFrame(index=X.index[(X.index >= start_time) & (X.index <= end_time)], columns=columns_out)
    for i, var in enumerate(vars):
        df_predict[var] = y[:, i]
    df_predict['NA_COUNT'] = na_count

    return df_predict


def main(config):
    """
    main prediction code
    :return:
    """
    predict_dir = config['PREDICTIONS_ROOT']#'/data/awn/impute/oper/predictions'
    obs_dir = config['OBS_ROOT']  # '/data/awn/impute/oper/obs'
    predict_start_date = pd.to_datetime(config['predict_start_date']) #2020-11-10
    data_start_date = pd.to_datetime(config['data_start_date'])
    vars = config['vars']

    now = datetime.utcnow() - timedelta(hours=7)
    # station must have been installed after this date to be counted

    # pull the list of STNIDs from estimator directory
    est_dir = config['ESTIMATORS_ROOT']
    est_files = glob('{}/*'.format(est_dir))
    stids = extract_stids(est_files)

    # pull the list of STNIDs from obs directory (not sure which of these I need yet)
    obs_files = glob('{}/*'.format(obs_dir))
    stids = extract_stids(obs_files)

    print('Compiling obs')

    # assemble the obs dataframe that is needed for making the predictions. Pull the columns from the most recent
    # trained metadata (in the metadata file). Index begins at the start time of predictions listed in config.
    df_features = pd.read_pickle('{}/metadata/estimator_features.p'.format(config['ROOT_DIR']))

    # to create df_obs we use all of the STIDs in the obs folder, then we trim it for each estimator using df_features
    estimator_columns = []
    for stid in stids:
        for var in vars:
            estimator_columns.append('{}_{}'.format(stid, var))

    #index = pd.date_range(start=data_start_date, end=now, freq='0.25H')
    # the use of predict_start_date here is problematic if a station stopped reporting data since the model was
    # trained, which could cause the imputer to drop it later. Currently hacked by moving back a few weeks
    index = pd.date_range(start=predict_start_date-timedelta(days=21), end=now, freq='0.25H')
    df_obs = pd.DataFrame(index=index, columns=estimator_columns)#columns=df_meta.columns)

    # Fill in df_obs
    for stid in stids:
        obs_tmp = pd.read_pickle('{}/obs_{}.p'.format(obs_dir, stid))
        obs_tmp = obs_tmp[obs_tmp.index >= predict_start_date-timedelta(days=21)]
        for var in vars:
            df_obs['{}_{}'.format(stid, var)] = obs_tmp['{}'.format(var)]


    print('Predicting for {} stations'.format(len(stids)))
    # loop through STIDS, check if exists, if not, create a new dataframe and fill in from predict_start date,
    # otherwise import the pickle file and predict from the most recent time up to the present
    for stid in stids:
        predict_file = '{}/predictions_{}.p'.format(predict_dir, stid)
        feature_columns = df_features['features'][stids[0]].values
        if os.path.isfile(predict_file):
            print('found existing predictions for {}'.format(stid))
            df = pd.read_pickle(predict_file)
            # always predict back by 2 extra days for when additional obs data flows in
            predict_start_date_tmp = df.index[-1] - timedelta(days=2)
            df_latest = predict_one_station(df_obs, feature_columns, est_dir, stid, vars,
                                            predict_start_date_tmp, now)
            # trim DF to delete old predictions
            df = df[df.index < df_latest.index[0]]
        else:
            print('no existing predictions for {}'.format(stid))
            df = pd.DataFrame(columns=vars)
            print('predicting from {}'.format(predict_start_date))
            df_latest = predict_one_station(df_obs, feature_columns, est_dir, stid, vars, predict_start_date, now)
            # save data
            if not df_latest.empty:
                print('made predictions from {} to {}'.format(predict_start_date, df_latest.index[-1]))

        df = pd.concat((df, df_latest))
        print('made predictions from {} to {}'.format(df_latest.index[0], df_latest.index[-1]))
        print('latest prediction: {}'.format(df.index[-1]))
        df.to_pickle(predict_file)
        print('saved {}'.format(stid))

    return