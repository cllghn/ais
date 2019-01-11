# -*- coding: utf-8 -*-
"""
.log to JSON
@author: corelab
"""
##############
# Background #
##############
"""
The purpose of this script is to create a function, which extracts relevant strings from the .log files, extracting the payload data, and saving the output into a JSON file for futher processing.
The function itself takes:
    * DF_LOG, which is the location of the .log files
NOTE: For this script, the DF_LOG has been commented out so you need to set up your own directory.
"""

import glob
import json
import os
import ais

#########################
# Set global parameters #
#########################

# Location of the .log data
#DF_LOG = '/Users/corelab/Dropbox/Python FY19/AIS/Test'

###################
# Define Function #
###################

def log_to_json(DF_LOG):
    os.chdir(DF_LOG)
    #logs = {}
    # loop through each item in the file with the extension .log
    for file in glob.glob('*.log'):
        print("Reading: ", file)
        f = open(file, "r")
        # Create a key for each file name
        logs[file] = {}
        # Craeate multiple lists for each wanted variable
        # Deeper explanation of the values selected found here: https://github.com/caribewave/vessels-monitoring/blob/master/README.md
        logs[file]['PType'] = [] # Type of payload: AIVDM, BSVDM, or SAVDM
        logs[file]['Payload'] = [] # Payload value
        logs[file]['Pad'] = [] # Number of bits required to pad the data payload
        logs[file]['Transmission'] = [] # Decoded AIS transmissions
        # Reading each line from log
        f_lines = f.read().splitlines()
        # Looping over each line to extract relevant values
        for line in f_lines:
            # filter out broken transmissions (e.g., more than 6 characters after splitting)
            if len(line.split(',')) > 6:
                type_value = line.split(',')[0]
                type_value = type_value.replace("!","")
                payload_value = line.split(',')[5]
                pad_value = line.split(',')[6]
                pad_value = pad_value.split('*')[0]
                logs[file]['PType'].append(type_value)
                logs[file]['Payload'].append(payload_value)
                logs[file]['Pad'].append(pad_value)
                # Try to decode otherwise record error
                try:
                    logs[file]['Transmission'].append(ais.decode(str(payload_value), int(pad_value)))
                    # NOTE: ais.decode requires the an str for the first value and a int for second value
                    print("Decoded succesfully!")
                #except Exception as e:
                except Exception as e:
                    print("Not decoded.")
                    logs[file]['Transmission'].append(str(e))
                    continue
        with open('data.json', 'w') as outfile:
            json.dump(logs, outfile)
            print(file, "Now attached to JSON!")

###########
# Run it! #
###########
#log_to_json(DF_LOG)
