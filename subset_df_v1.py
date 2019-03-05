#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  4 12:27:04 2019

@author: corelab
"""
import os
import pandas as pd
import kvswarm as kv

#########################
# Set global parameters #
#########################
# Location of the .csv.gz data
DF_CSV = '/home/chris/Dropbox/GitHub/ais/csv'

# Location of output
DF_OUT = '/home/chris/Dropbox/GitHub/ais/csv/bboxsubset'

# Location to store KV-Swarm database      
DF_DB = "/dev/shm"

# Set max size of Nest containers
MAX_NEST_SIZE = '4 GB'

# Set max lines decoded per file
MAX_LINES = 1e05

# Area of interest bounding box
bbox = {
      'baltic': {
              'ymin': 53.5,
              'ymax': 65.6,
              'xmin': 9.2,
              'xmax': 31.1}
      }
      
      
###################
# Define function #
###################
def subset_df(file_name, bbox=bbox, output_folder=None, max_lines=None):
    
    # Read file
    print("Reading file: {}".format(file_name))
    dat = pd.read_csv(file_name)
    
    # Drop observations without x and y
    dat = dat.dropna(subset=['x','y'])
    print("Records geocoded world-wide: {}".format(len(dat)))
    
    # Subset records using the bounding box
    dat = dat[(dat['x'] >= int(bbox['baltic']['xmin'])) & (dat['x'] <= int(bbox['baltic']['xmax'])) & (dat['y'] >= int(bbox['baltic']['ymin'])) & (dat['y'] <= int(bbox['baltic']['ymax']))]
    
    # Write CSV
    new_name = '{}.bboxsubset.v5.csv.gz'.format(file_name.rsplit('.')[0])
    if output_folder:
        new_name = os.path.join(output_folder, os.path.basename(new_name))
    dat.to_csv(new_name, index=False, compression='gzip')
    print("Finished writing file: {} [{} records included]".format(new_name, len(dat)))
    
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
    sw.pApply(subset_df, file_list, output_folder=DF_OUT, max_lines=MAX_LINES)

    # Record execution time
    t.check(total=True)

    # Example usage:
    # >>> python log_to_df_v5.py