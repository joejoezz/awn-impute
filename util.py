"""
util file
"""

import os
import pdb
import re

def get_config(config_path):
    """
    Retrieve the config dictionary from config_path.
    Written by Jonathan Weyn jweyn@uw.edu
    Modified by Joe Zagrodnik joe.zagrodnik@wsu.edu
    """
    import configobj
    from validate import Validator
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_spec = '%s/configspec' % dir_path
    try:
        config = configobj.ConfigObj(config_path, configspec=config_spec, file_error=True)
    except IOError:
        print('Error: unable to open configuration file %s' % config_path)
        raise
    except configobj.ConfigObjError as e:
        print('Error while parsing configuration file %s' % config_path)
        print("*** Reason: '%s'" % e)
        raise

    config.validate(Validator())

    return config


def extract_stids(files):
    # extract stids from a list of files
    stids = []
    p = re.compile("\\d+")
    for file in files:
        stids.append(p.findall(file)[0])
    return stids