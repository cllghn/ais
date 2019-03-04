# -*- coding: utf-8 -*-
import ais
import os
import kvswarm as kv
import pandas as pd
from datetime import datetime

#########################
# Set global parameters #
#########################

# Location of the .log data
# DF_LOG = '/Users/corelab/Dropbox/Python FY19/AIS/Test'
# DF_LOG = '/home/chris/Dropbox/GitHub/ais/DataOne'
DF_LOG = "C:\\Users\\cjcallag\\Desktop\\AIS\\09"

# Location of CSV output
DF_CSV = "C:\\Users\\cjcallag\\Desktop\\AIS\\outputs"

# Location to store KV-Swarm database
DF_DB = "C:\\Users\\cjcallag\\Desktop\\AIS\\DB"
# DF_DB = "/dev/shm"

# Set max size of Nest containers
MAX_NEST_SIZE = '4 GB'

# Set max lines decoded per file
MAX_LINES = None

# Bounding box
bbox = {
      'baltic': {
              'ymin': 53.5,
              'ymax': 65.6,
              'xmin': 9.2,
              'xmax': 31.1}}

###################
# Define function #
###################
def log_to_csv(file_name, output_folder=None, max_lines=None, step=10000,
               bbox=bbox):

    # Read file
    print("Reading file: {}".format(file_name))
    dat = kv.read_file(file_name, as_text=True)

    # Split lines into list
    lines = dat.splitlines()
    line_cnt = len(lines)

    # Create queue for decoding AIS messages
    q = ais.nmea_queue.NmeaQueue()

    # Create empty list for output
    out = []
    cnt = 0

    # Loop over lines
    for msg in lines:
        cnt += 1
        if cnt % step == 0:
            print("Decoding Line #: {} of {}".format(cnt, line_cnt))
            if max_lines and cnt >= max_lines:
			     break

        # Add line to queue
        q.put(msg)

        # Check if multi-line message is complete
        if q.qsize():

            # Get decoded message as dict
            d = q.get()
            if 'decoded' in d:
                rec = d['decoded']

                # Check for additional metadata
                if 'matches' in d:
                    m = d['matches']

                    # Loop over list of sub-messages
                    for i in range(len(m)):
                        mi = m[i]

                        # Update record with additional fields
                        for key in mi:
                            val = mi[key]
                            if not val:
                                continue
                            k0 = 'm.' + str(i) + '.'
                            newkey = k0 + key
                            rec[newkey] = mi[key]

                            # Convert integer time to date-time string
                            if key == 'time':
                                dkey = k0 + 'datetime'
                                rec[dkey] = datetime.utcfromtimestamp(
                                    val).strftime('%Y-%m-%d %H:%M:%S')

                # Filter record to include geocoded within bbox
                if 'x' and 'y' in rec:
                    if (rec['x'] >= bbox['baltic']['xmin'] and
                        rec['x'] <= bbox['baltic']['xmax'] and
                        rec['y'] >= bbox['baltic']['ymin'] and
                        rec['y'] <= bbox['baltic']['ymax']):
                        out.append(rec)
                        
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(out)

    # Write CSV
    new_name = '{}.v6.csv.gz'.format(file_name.rsplit('.', 1)[0])
    if output_folder:
        new_name = os.path.join(output_folder, os.path.basename(new_name))
    df.to_csv(new_name, index=False, compression='gzip')
    print("Finished writing file: {} [{} messages decoded]".format(new_name, len(out)))


############################
# MAIN PROGRAM STARTS HERE #
############################

if __name__ == "__main__":

    # Create list of input files
    file_list = kv.get_paths(DF_LOG, '.log')

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
    sw.pApply(log_to_csv, file_list, output_folder=DF_CSV, max_lines=MAX_LINES)

    # Record execution time
    t.check(total=True)

    # Example usage:
    # >>> python log_to_df_v5.py
