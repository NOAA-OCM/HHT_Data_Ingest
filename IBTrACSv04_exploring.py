# -*- coding: utf-8 -*-
"""
IBTrACS_v04_exploring.py

Takes a CSV-formatted IBTrACSv04 file from
https://www.ncdc.noaa.gov/ibtracs/index.php?name=ib-v4-access
and extracts the columns needed for populating the NOAA OCM
Historical Hurricane Tracks web site:
    coast.noaa.gov/hurricanes

Created on Mon Jul 15 12:50:34 2019
    7/18/2019 Playing with Pandas

@author: Dave.Eslinger
"""

import pandas as pd
#import datetime as dt

workDir = "K:/GIS/Hurricanes/HHT/2018_Season/" # C: at work, K: at home
dataDir = workDir + "Data/"  # Data location

ibRaw = dataDir + "ibtracs.ALL.list.v04r00.csv"
ibdf = pd.read_csv(dataDir + "ibtracs.ALL.list.v04r00.csv", na_filter = False)
#ibRaw = dataDir + "ibTESTv04r00.csv"
#ibdf = pd.read_csv(dataDir + "ibTESTv04r00.csv", na_filter = False)
print(ibdf.head(10))
print(ibdf.info)
ibdf.LAT.unique()

smalldf = ibdf.loc[(ibdf['SEASON'] == 2015) & (ibdf['BASIN'] != 'NA') & (ibdf['BASIN'] != 'EP')]
print(smalldf.head(10))
print(smalldf.info)
smalldf.USA_WIND.unique()  # WOOT!  Appears to be no missing values in this column
smalldf.USA_PRES.unique()  # WOOT!  Appears to be no missing values in this column
#smalldf.USA_WIND.unique()  # WOOT!  Appears to be no missing values in this column
print(smalldf)

ibUSA = ibdf.filter(regex=r'USA')
ibUSA.head(3)
bar = ibUSA.drop(ibUSA.index[0])
bar.head(3)
print(ibUSA.index[0])
ibUSA.info()

foo = ibUSA[ibUSA.USA_LAT != 'degrees_north'] #.index[0])
foo.head(3)
print(foo)
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