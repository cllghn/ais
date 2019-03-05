#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import itertools
import pandas as pd
import kvswarm as kv
import numpy as np

#########################
# Set global parameters #
#########################
# Location of the .csv.gz data
DF_CSV = '/home/chris/Dropbox/GitHub/ais/csv/bboxsubset'

# Location of output
DF_OUT = '/home/chris/Dropbox/GitHub/ais/csv/bboxsubset/grid'

# Location to store KV-Swarm database
DF_DB = "/dev/shm"

# Set max size of Nest containers
MAX_NEST_SIZE = '4 GB'

# Set max lines decoded per file
MAX_LINES = 1e05

bbox = {
      'baltic': {
              'ymin': 53.5,
              'ymax': 65.6,
              'xmin': 9.2,
              'xmax': 31.1}}


###################
# Define function #
###################
def grid_counter(file_name, bbox=bbox, output_folder=None, max_lines=None):

    # Set grid resolution
    GRES = 0.1

    # Set time resolution
    TRES = 'hours'

    # Convert floating point number to integer string
    # e.g. 12.543 --> '125'
    def float_to_intstr(x, res=GRES):
        return str(int(x/res))

    # Convert integer string to floating point string
    # e.g. '125' --> '12.5'
    def intstr_to_fstr(x, res=GRES):
        return str(float(x)*res)

    # Convert date to shortened string
    # e.g. '2018-10-05 12:45:56' --> '2018-10-05 12'
    def date_to_str(x, res=TRES):
        schar = ':'
        nsplit = 1
        if TRES == 'minutes':
            pass
        elif TRES == 'hours':
            nsplit = 2
        elif TRES == 'days':
            schar = ' '
        else:
            print("Error in date_to_str: Time units not recognized")
            return
        return str(x).rsplit(schar, nsplit)[0]

    # Get range of date strings with specified units
    def get_dates(sdate, edate, unit='days'):
        if unit == 'minutes':
            sdate = sdate.rsplit(':', 1)[0] + ':00'
            edate = edate.rsplit(':', 1)[0] + ':59'
            dates = pd.date_range(sdate, edate, freq='1min').tolist()
        elif unit == 'hours':
            sdate = sdate.split(' ')[0] + ' 00:00'
            edate = edate.split(' ')[0] + ' 23:59'
            dates = pd.date_range(sdate, edate, freq='1H').tolist()
        elif unit == 'days':
            dates = pd.date_range(sdate, edate, freq='1d').tolist()
            dates = [str(x).split(' ')[0] for x in dates]
        dates = [str(x) for x in dates]
        return dates

    # Setup
    xmin = bbox['baltic']['xmin']
    xmax = bbox['baltic']['xmax']
    ymin = bbox['baltic']['ymin']
    ymax = bbox['baltic']['ymax']
    count = {}
    out = []

    # Read file
    print("Reading file: {}".format(file_name))
    dat = pd.read_csv(file_name)
    dat = dat.dropna(subset=['x', 'y'])
    dates = dat['m.0.datetime']

    # Iterate over dataframe rows
    for index, row in dat.iterrows():
        if max_lines and index > max_lines:
            break

        # Limit to bounding box
        x = row['x']
        y = row['y']
        if x < xmin or x > xmax or y < ymin or y > ymax:
            continue

        # Create time-location key
        x = float_to_intstr(x)
        y = float_to_intstr(y)
        dt = date_to_str(row['m.0.datetime'])
        key = '_'.join([dt, x, y])

        # Count keys
        if key not in count:
            count[key] = 0
        count[key] += 1

    # Make a list of all valid keys
    val_x = [float_to_intstr(i) for i in np.arange(xmin, xmax+0.0001, GRES)]
    val_y = [float_to_intstr(i) for i in np.arange(ymin, ymax+0.0001, GRES)]
    val_dt = [date_to_str(i) for i in get_dates(
        sdate=min(dates), edate=max(dates), unit=TRES)]
    key_list = ['_'.join(i) for i in
                sorted(itertools.product(val_dt, val_x, val_y))]

    # Convert to list of dicts, inserting zeros for missing keys
    for key in key_list:
        dt, x, y = key.split('_')
        date, time = dt.split(' ')
        x = intstr_to_fstr(x)
        y = intstr_to_fstr(y)
        d = {'key': key, 'date': date, 'time': time, 'x': x, 'y': y}
        if key in count:
            d['count'] = str(count[key])
        else:
            d['count'] = '0'
        out.append(d)

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(out)

    # Write CSV
    new_name = '{}.grid.bboxsubset.v5b.csv.gz'.format(file_name.rsplit('.')[0])
    if output_folder:
        new_name = os.path.join(output_folder, os.path.basename(new_name))
    df.to_csv(new_name, index=False, compression='gzip',
              columns=['key', 'date', 'time', 'x', 'y', 'count'])
    print("Finished writing file: {} [input: {} rows, output: {} rows]".format(
        new_name, len(dat), len(out)))


############################
# MAIN PROGRAM STARTS HERE #
############################
if __name__ == "__main__":

    # Create list of input files
    file_list = kv.get_paths(DF_CSV, '.csv.gz')

    # Create Logger object
    # (this will copy all console output to a logfile in the working directory)
    logger = kv.Logger()

    # Create Swarm object
    # (this clears any previous created Swarms in this folder)
    sw = kv.Swarm(clear=True, path=DF_DB, maxsize=MAX_NEST_SIZE, logger=logger)

    # Start timer
    t = kv.Timer()

    # Execute function in parallel
    #  (First arg is a function, second arg is a sequence.
    #  Each worker executes the function once, taking a single
    #  element from the sequence, and passing this as the first
    #  argument to the function.  Any addtional keyword arguments
    #  are passed on as keyword arguments to the function)
    sw.pApply(grid_counter, file_list, output_folder=DF_OUT, max_lines=MAX_LINES)

    # Record execution time
    t.check(total=True)
