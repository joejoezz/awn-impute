"""
Script to run the AWN imputer
Standard procedure on a crontab would be: 'python run config.conf --download_obs --predict --qaqc --plot'
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
import download_obs
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
    if not os.path.isdir('{}/metadata2'.format(root_dir)):
        os.mkdir('{}/metadata2'.format(root_dir))

    pdb.set_trace()

    # --- download data ---
    if args.clean:
        clean.main(config)
    else:
        print('skipping database cleaning')
    # --- download data ---
    if args.download:
        download_obs.main(config)
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







