"""
Plotting script
"""

import pdb
from glob import glob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, DateFormatter, MonthLocator
import os
import re
from util import extract_stids


def plot_one_station(df_qaqc, stid, vars, flag_thresholds, now, plot_dir):
    """
    make QAQC monitoring plots
    :param df_qaqc: qaqc df
    :param stid: station ID
    :param vars: variables we are QCing
    :prarm flag_thresholds: QAQC thresholds
    :param now: current time
    :return:
    """

    # trim to most recent 2 days
    df_qaqc = df_qaqc[df_qaqc.index > now-timedelta(days=2)]

    fig, axs = plt.subplots(nrows=len(vars), ncols=1)
    fig.set_size_inches(8, 5)

    for i in range(0,len(vars)):
        var = vars[i]
        ax = axs[i]
        impute = df_qaqc['{}_IMPUTE'.format(var)]
        obs = impute - df_qaqc['{}_DIFFERENCE'.format(var)]
        rolling = abs(df_qaqc['{}_ROLLING_DIFFERENCE'.format(var)])
        axb = ax.twinx()
        p1 = ax.plot(obs.index, obs, color='black', label='OBS', lw=1)
        p2 = ax.plot(impute.index, impute, color='blue', label='IMPUTE', lw=1)
        p3 = axb.plot(rolling.index, rolling, color='red')

        ax.set_ylabel('{}'.format(var), color='black')
        axb.set_ylabel('Abs. Rolling Difference', color='red')
        if np.max(rolling) < flag_thresholds[i]:
            axb.set_ylim(0,flag_thresholds[i])
        else:
            axb.set_ylim(0,np.max(rolling)*1.1)
        axb.tick_params(axis='y', labelcolor='red')
        ax.grid()
        ax.xaxis.set_major_locator(DayLocator())
        ax.xaxis.set_major_formatter(DateFormatter('%b-%d'))
        ax.legend()

    # plt.annotate('Data: Iowa Mesonet RAOB Archive   Plot: Joe Zagrodnik',(145,430),ha='right',fontsize=5)

    plt.savefig('{}/{}_plot.png'.format(plot_dir, stid), bbox_inches='tight', dpi=200)
    #pdb.set_trace()

    return

def main(config):
    """
    main prediction code
    :return:
    """
    qaqc_dir = config['QAQC_ROOT']  # '/data/awn/impute/oper/qaqc'
    plot_dir = config['PLOT_ROOT']
    vars = config['vars']
    flag_thresholds = [float(i) for i in config['flag_thresholds']]

    now = datetime.utcnow() - timedelta(hours=7)

    # pull the list of STNIDs from obs and predictions directories
    # we only run the QAQC checks if we have both obs and predictions
    qaqc_files = glob('{}/*'.format(qaqc_dir))
    stids = extract_stids(qaqc_files)

    print('Plotting for {} stations'.format(len(stids)))
    # loop through * STIDS and call the QAQC function
    for stid in stids:
        qaqc_file = '{}/qaqc_{}.csv'.format(qaqc_dir, stid)
        df_qaqc = pd.read_csv(qaqc_file, index_col=0)
        df_qaqc.index = pd.to_datetime(df_qaqc.index)
        plot_one_station(df_qaqc, stid, vars, flag_thresholds, now, plot_dir)
        print('saved  plot {}'.format(stid))

    return