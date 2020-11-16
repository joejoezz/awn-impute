"""
Script to run the AWN imputer
Standard procedure on a crontab would be: 'python run config.conf --download --predict --qaqc --plot'
Occasionally 'train' should be run to update the models but needs to be used sparingly.

To-do:
-Script to clean old stations that are no longer active when model imputes
"""

import sys
import os
import time
import warnings
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pdb
import util
from argparse import ArgumentParser
import clean
import download
import train
import predict
import qaqc
import plot


def main(args):
    """
    Main running script
    """

    # Get the config file
    config = util.get_config(args.config)
    root_dir = config['ROOT_DIR']
    # fill out initial folders
    if not os.path.isdir('{}/metadata'.format(root_dir)):
        os.mkdir('{}/metadata'.format(root_dir))
        print('created metadata dir')
    if not os.path.isdir('{}'.format(config['OBS_ROOT'])):
        os.mkdir('{}'.format(config['OBS_ROOT']))
        print('created OBS dir')
    if not os.path.isdir('{}'.format(config['ESTIMATORS_ROOT'])):
        os.mkdir('{}'.format(config['ESTIMATORS_ROOT']))
        print('created ESTIMATORS dir')
    if not os.path.isdir('{}'.format(config['PREDICTIONS_ROOT'])):
        os.mkdir('{}'.format(config['PREDICTIONS_ROOT']))
        print('created PREDICTIONS dir')
    if not os.path.isdir('{}'.format(config['QAQC_ROOT'])):
        os.mkdir('{}'.format(config['QAQC_ROOT']))
        print('created QAQC dir')
    if not os.path.isdir('{}'.format(config['PLOT_ROOT'])):
        os.mkdir('{}'.format(config['PLOT_ROOT']))
        print('created PLOT dir')

    # --- download data ---
    if args.clean:
        clean.main(config)
    else:
        print('skipping database cleaning')
    # --- download data ---
    if args.download:
        download.main(config)
    else:
        print('skipping download of new data')
    # --- train models
    if args.train:
        train.main(config)
    else:
        print('skip training')
    # --- make predictions ---
    if args.predict:
        predict.main(config)
    else:
        print('skipping download of new data')
    # --- run qaqc checks ---
    if args.qaqc:
        qaqc.main(config)
    else:
        print('skipping qaqc')
    # --- plot ---
    if args.plot:
        plot.main(config)
    else:
        print('skipping plots')







