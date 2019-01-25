# -*- coding: utf-8 -*-
"""
.log to JSON
@author: corelab
"""
##############
# Background #
##############
"""
The purpose of this script is to create a function, which extracts relevant
strings from the .log files, extracting the payload data, and saving the output
into a JSON file for futher processing.
The function itself takes:
    * DF_LOG, which is the location of the .log files
NOTE: For this script, the DF_LOG has been commented out so you need to set up
 your own directory.
"""

import glob
import os
import ais
import pandas as pd

#########################
# Set global parameters #
#########################

# Location of the .log data
#DF_LOG = '/Users/corelab/Dropbox/Python FY19/AIS/Test'
DF_LOG = '/home/chris/Dropbox/GitHub/ais/DataOne'

###################
# Define Function #
###################

def log_to_df(DF_LOG):
    os.chdir(DF_LOG)
    logs = {}
    df = pd.DataFrame()
    n=0
    # loop through each item in the file with the extension .log
    for file in glob.glob('*.log'):
        print("Reading: ", file)
        f = open(file, "r")
        # Create a key for each file name
        logs[file] = {}
        # Craeate multiple lists for each wanted variable
        # Deeper explanation of the values selected found here: https://github.com/caribewave/vessels-monitoring/blob/master/README.md
        logs[file]['Transmission'] = [] # Decoded AIS transmissions
        logs[file]['Test'] = [] # Decoding test
        # Reading each line from log
        f_lines = f.read().splitlines()
        print("There are ", len(f_lines), "records in the document...")
        # Looping over each line to extract relevant values
        for line in f_lines:
            # filter out broken transmissions (e.g., more than 6 characters after splitting)
            if len(line.split(',')) > 6:
                payload_value = line.split(',')[5]
                pad_value = line.split(',')[6]
                pad_value = pad_value.split('*')[0]
                # Try to decode otherwise record error
                try:
                    logs[file]['Transmission'].append(ais.decode(str(payload_value), int(pad_value)))
                    logs[file]['Test'].append('Coded')
                    # NOTE: ais.decode requires the an str for the first value and a int for second value
                    print("Decoded succesfully!")
                #except Exception as e:
                except Exception as e:
                    print("Not decoded.")
                    logs[file]['Transmission'].append(False)
                    logs[file]['Test'].append(str(e))
                    continue 
        for values in logs[file]['Transmission']:
            if values != False:
                try:
                    print("### Appending Transmission ###")
                #print(type(values))
                    data = pd.DataFrame(data=values,
                                        index=[n+1])
                    df = df.append(data)
                except Exception:
                    continue
        df.to_csv('unpacked-%s.csv.gz' % str(file),
          sep=',',
          header=True,
          index=False,
          compression='gzip',
          quotechar='"',
          doublequote=True,
          line_terminator='\n')
                
            
###########
# Run it! #
###########
log_to_df(DF_LOG)
