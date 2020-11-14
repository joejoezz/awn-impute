"""
pull all obs into one large dataframe
train RF estimators for each site -- one estimator per site
"""

import glob
import sys
import os
import time
import pdb
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from joblib import dump, load


def fit_predict_one_station(X, y, stnid, n_estimators):
    """
    :param X: predictors
    :param y: predictand
    :param stnid: station ID
    :param var: variable we're predicting
    :return: nothing
    """

    # do a train test split -- test size is a mere 5 just to make sure our model works
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=5, random_state=42)

    # drop where y_train is NA
    y_train_na = y_train.index[y_train.isna().any(axis=1) == True]
    X_train.drop(index=y_train_na, inplace=True)
    y_train = y_train.drop(index=y_train_na)

    print('Building model for {}'.format(stnid))
    # build regression model.
    regr = RandomForestRegressor(max_depth=None, random_state=0, verbose=2, n_estimators=n_estimators, n_jobs=30)
    # train model
    regr.fit(X_train, y_train)
    # make predictions
    y_predict = regr.predict(X_test)
    df_predict = pd.DataFrame(data=y_predict, index=y_test.index, columns=y_test.columns)
    print('Made 5 test predictions ')
    print('Actual: {}'.format(y_test))
    print('Predict: {}'.format(df_predict))

    # Save the values
    savedir = '/data/awn/impute/oper/estimators'
    filename = '{}/estimator_{}.joblib'.format(savedir, stnid)
    dump(regr, filename)
    print('Saved {}'.format(filename))
    return


def main(config):
    """
    main training code
    """
    print('Training the estimators')
    n_estimators = config['n_estimators']
    obs_dir = config['OBS_ROOT']
    est_dir = config['ESTIMATORS_ROOT']
    vars = config['vars']
    data_start_date = pd.to_datetime(config['data_start_date'])

    now = datetime.utcnow()-timedelta(hours=7)
    two_days_ago = datetime.utcnow()-timedelta(hours=7)-timedelta(days=2)

    # assemble the big dataframe
    obs_files = glob.glob('{}/*'.format(obs_dir))
    columns = []
    stids = []
    for fl in obs_files:
        stid = fl.split('_')[1].split('.')[0]
        stids.append(stid)
        for var in vars:
            columns.append('{}_{}'.format(stid, var))

    # create big dataframe
    index = pd.date_range(start=datetime(2012, 1, 1, 0, 0), end=now, freq='0.25H')
    df = pd.DataFrame(index=index, columns=columns)

    # populate big dataframe
    for fl in obs_files:
        stid = fl.split('_')[1].split('.')[0]
        df_tmp = pd.read_pickle(fl)
        for var in vars:
            df['{}_{}'.format(stid, var)] = df_tmp[var]

    df.dropna(how='all', inplace=True)

    # trim df to drop the most recent 48 hours, which are only used for QA/QC checks
    df = df[(df.index < two_days_ago)]

    # impute missing and make X array
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    X_simple = imp_mean.fit_transform(df)
    df_simple = pd.DataFrame(index=df.index, columns=df.columns)
    df_simple[:] = X_simple

    columns = df.columns

    # save the features list, which is a record of which stations we used. If it doesn't exist, create a new one
    # this is useful for making sure the estimator uses the right 'X' when predicting
    features_file = '{}/metadata/estimator_features.p'.format(config['ROOT_DIR'])
    if os.path.exists(features_file):
        print('loading previous features metadata file')
        df_features = pd.read_pickle(features_file)
    else:
        print('making new features metadata file')
        df_features = pd.DataFrame(index=stids, columns=['features', 'update_date'])

    # loop through and train model
    for stid in stids:
        # get columns for this stid
        columns_tmp = []
        for var in vars:
            columns_tmp.append('{}_{}'.format(stid, var))
        # create an X without columns from this stid
        X = df_simple.drop(columns=columns_tmp)
        # now train the model for each variable

        # create a Y for the station we're training on
        y = df[columns_tmp]
        fit_predict_one_station(X, y, stid, n_estimators)

        # update features df
        if stid not in df_features.index:
            df_tmp = pd.DataFrame(index=[stid], columns=df_features.columns)
            df_features = pd.concat((df_features, df_tmp))
        df_features['features'][stid] = df_simple.columns
        df_features['update_date'][stid] = now.date()
        df_features.to_pickle('{}/metadata/estimator_features.p'.format(config['ROOT_DIR']))

    return