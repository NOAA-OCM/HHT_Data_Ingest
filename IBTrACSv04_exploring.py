# -*- coding: utf-8 -*-
"""
IBTrACS_v04_exploring.py

Takes a CSV-formatted IBTrACSv04 file from
https://www.ncdc.noaa.gov/ibtracs/index.php?name=ib-v4-access
and extracts the columns needed for populating the NOAA OCM
Historical Hurricane Tracks web site:
    coast.noaa.gov/hurricanes

Created on Mon Jul 15 12:50:34 2019

@author: Dave.Eslinger
"""

#import os

workDir = "C:/GIS/Hurricanes/HHT/2018_Season/" # On OCM Work Machine
dataDir = workDir + "Data/"  # Data location

ibRaw = dataDir + "ibtracs.ALL.list.v04r00.csv"

with open(ibRaw, "r") as rawObsFile :
    headers = rawObsFile.readline().split(",")
    units = rawObsFile.readline().split(",")
    # head3 = ib.readline()
    # head4 = ib.readline()
    """ Read first IBTrACS Record """
    lineVals = rawObsFile.readline() # First Storm record in IBTrACS
    vals = lineVals.split(",")
    while (vals[1] != "2018"):
        lineVals = rawObsFile.readline() # First Storm record in IBTrACS
        vals = lineVals.split(",")
    # Found some 2018 data
    #print(headers[0:26])
    #print(units[0:26])
    print(headers[0:12], headers[19:21], headers[23:26])
    #while True: # Read and print out the "USA" 2018 data
    while vals[1] != "2018": # Read and print out the "USA" 2018 data
        lineVals = rawObsFile.readline()
        if not lineVals: # Finds EOF
            break # Break on EOF
        else: # Data read: Parse it and test to see if it is a new storm
            vals = lineVals.split(",")
            print(vals[0:12], vals[19:21], vals[23:26])
        