# -*- coding: utf-8 -*-
"""
First test for reading and reformatting 3 files for HHT data reprocessing.
Python 3.4

Created 2014-12-11 by David L Eslinger (DLE)
    NOAA Office for Coastal Management
    Charleston, SC USA
    
Revised:

"""
#import csv

""" Declarations and Parameters """
workDir = "C:/GIS/Hurricane/HHT_Python/" # On Work Machine
#workDir = "/home/dave/Data/Hurricanes/" # On ZOG
dataDir = workDir  # Main Data location
h2AtlRaw = dataDir + "h2ATL.txt"     # HURDAT2 North Atlantic Data
h2nepacRaw = dataDir + "h2NEPAC.txt" # HURDAT2 NE North Pacific Data
ibRaw = dataDir + "IB.csv"           # IBTrACS CSC version Data

resultsDir = workDir + "Results/"  #  Location for final data

""" Storm Object """
class Storm(object):
    maxw = None
    minp = None
    nobs = 0
    obs = []
    start_time = ""
    end_time = ""
    sstrack = ""
    def __init__(self,uid,name):
        self.uid = uid
        self.name = name

class Observation(object):
    def __init__(self,time,lat,lon,wsp,pres,nat,ss):
        self.time = time
        self.lat = lat
        self.lon = lon
        self.wsp = wsp
        self.pres = pres
        self.nat = nat
        self.ss = ss

""" Create an empty list to hold allStorms
    and initialize the total storm counter """
allStorms = []
numStorms = -1

""" Read and QC/QA HURDAT2 data """
hFiles = [h2AtlRaw, h2nepacRaw]
#hFiles = [h2AtlRaw]
for i, file in enumerate(hFiles):
    print (i, file)
    with open(file, "r") as h2in:
        """h2reader = csv.reader(h2in, delimiter=",")
        for row in h2reader:
            print(row)"""
        """ Need a manual loop here to read a header record then 
        the rest of the observations """   
        while True: # With this and the below break, read to EOF
            lineVals = h2in.readline()
            if not lineVals: # Finds EOF
                break # Break on EOF

            """ This is a new storm so create a new storm record for it """
            numStorms += 1
            vals = lineVals.split(",")
            #print ("vals = ",vals[0],vals[1],vals[2], len(vals))
            thisStorm = Storm(vals[0],vals[1])  # Create new storm using Unique ID and Name
            thisStorm.nobs = vals[2] # Number of Observations
            print(thisStorm.uid, thisStorm.name, thisStorm.nobs)

            for ob in range(int(thisStorm.nobs)):
                lineVals = h2in.readline()
                if not lineVals: # Finds EOF
                    break # Break on EOF
                """ Create a new observation record """
                vals = lineVals.split(", ") # Split the record into fields
                """ Format 2 time fields to one ISO format """
                otime = vals[0][0:4] +"-"+vals[0][4:6]+"-"+vals[0][6:] + " "
                otime += vals[1][:2] + ":" + vals[1][2:] + ":00"

                lon = float(vals[4][:4]) if vals[4][4] == "N" else -1.*float(vals[4][:4])
                lat = float(vals[5][:5]) if vals[5][5] == "E" else -1.*float(vals[5][:5])
                print(otime, lon, lat)
                thisObs = Observation(otime,lat,lon,vals[6],vals[7],vals[3],None)
                thisStorm.obs.append(thisObs)
            """ Should be done with reading new storm data 
                Now add it to the allStorms data if appropriate """
            allStorms.append(thisStorm)
            print ("Storm", allStorms[numStorms].name,"has ", len(allStorms[numStorms].obs))

""" End of HURDAT2 Ingest and QC/QA """   

""" Read and QC/QA IBTrACS data """

""" End of IBTrACS Ingest and QA/QC """

""" Combine files into one set of storms"""

""" For each storm : """
"""     Create segment shapefiles """

"""     Create track shapefiles """

""" Write out shapefiles """