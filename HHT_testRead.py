# -*- coding: utf-8 -*-
"""
First test for reading and reformatting 3 files for HHT data reprocessing.
Python 3.4

Created 2014-12-11 by David L Eslinger (DLE)
    NOAA Office for Coastal Management
    Charleston, SC USA
    
Revised: 2014-12-18: HURDAT2 import working (DLE)

"""
import csv

""" Declarations and Parameters """
#workDir = "C:/GIS/Hurricane/HHT_Python/" # On Work Machine
workDir = "/home/dave/Data/Hurricanes/" # On ZOG
dataDir = workDir  # Main Data location
h2AtlRaw = dataDir + "h2ATL.txt"     # HURDAT2 North Atlantic Data
h2nepacRaw = dataDir + "h2NEPAC.txt" # HURDAT2 NE North Pacific Data
ibRaw = dataDir + "IB.csv"           # IBTrACS CSC version Data

resultsDir = workDir + "Results/"  #  Location for final data

""" Storm Object """
class Storm(object):
    def __init__(self,uid,name):
        self.uid = uid
        self.name = name
        self.maxw = -99
        self.minp = 999
        self.numsegs = 0
        self.segs = []
        self.start_time = None
        self.end_time = None
        self.maxsafir = ""

class Observation(object):
    def __init__(self,time,lat,lon,wsp,pres,nature):
        self.time = time
        self.startLat = lat
        self.startLon = lon
        self.wsp = wsp
        self.pres = pres
        self.nature = nature

class Segment(Observation):
    def __init__(self,time,lat,lon,wsp,pres,nature):
        self.endLat = None
        self.endLon = None
        self.safir = None

""" Create an empty list to hold allStorms
    and initialize the total storm counter """
allStorms = []
numStorms = -1

""" Read HURDAT2 data """
hFiles = [h2AtlRaw, h2nepacRaw]
#hFiles = [h2AtlRaw]
for i, file in enumerate(hFiles):
    print (i, file)
    with open(file, "r") as rawObsFile:
        """h2reader = csv.reader(rawObsFile, delimiter=",")
        for row in h2reader:
            print(row)"""
        """ Need a manual loop here to read a header record then 
        the rest of the observations """   
        while True: # With this and the below break, read to EOF
            lineVals = rawObsFile.readline()
            if not lineVals: # Finds EOF
                break # Break on EOF

            """ This is a new storm so create a new storm record for it """
            numStorms += 1
            vals = lineVals.split(",")
            #print ("vals = ",vals[0],vals[1],vals[2], len(vals))
            thisStorm = Storm(vals[0],vals[1])  # Create new storm using Unique ID and Name
            thisStorm.nobs = vals[2] # Number of Observations
            #print(thisStorm.uid, thisStorm.name, thisStorm.nobs)

            for ob in range(int(thisStorm.nobs)):
                lineVals = rawObsFile.readline()
                if not lineVals: # Finds EOF
                    break # Break on EOF
                """ Create a new observation record """
                vals = lineVals.split(", ") # Split the record into fields
                """ Format 2 time fields to one ISO format """
                otime = vals[0][0:4] +"-"+vals[0][4:6]+"-"+vals[0][6:] + " "
                otime += vals[1][:2] + ":" + vals[1][2:] + ":00"

                lon = (float(vals[4][:4]) if vals[4][4] == "N" 
                         else -1. * float(vals[4][:4]))
                lat = (float(vals[5][:5]) if vals[5][5] == "E" 
                        else -1. * float(vals[5][:5]))
                #print(otime, lon, lat)
                observation = Segment(otime,lat,lon,vals[6],
                                      vals[7],vals[3])
                thisStorm.segs.append(observation)
            
            """ All observations read for this new storm data 
                add thisStorm to the allStorms """
#==============================================================================
#             print ("thisStorm name ", thisStorm.name,"has",
#                    thisStorm.nobs, "observations and is index ", numStorms)            
#==============================================================================
            allStorms.append(thisStorm)
#==============================================================================
#             print ("Storm number ", len(allStorms)," named ",
#                    allStorms[numStorms].name,"has ", 
#                    len(allStorms[numStorms].segs), allStorms[numStorms].nobs)
#==============================================================================
""" End of HURDAT2 Ingest"""   

""" Read IBTrACS data 
    This data is not split by storms, rather every row has all info in it
    Therefore, we must read the data, find out if it is a new storm, and
    if it is, then write the previous storm object/information to allStorms 
    and create a new thisStorm object and populate it with the first 
    observation.
    
    We know the IBTrACS data starts with 3 header rows, then the 4th row
    is our first legitimate data record.  
    Initialize the first thisStorm object from that"""
    
with open(ibRaw, "r") as rawObsFile:
    h2reader = csv.reader(rawObsFile, delimiter=",")
    head1 = h2reader
    head2 = h2reader
    head3 = h2reader
    record = h2reader
    print(head1, head2, head3, record)
    for i, row in enumerate(h2reader):
        print(row)
        if i > 5:
            break
        else:
            pass
        
""" End of IBTrACS Ingest """

""" Combine files into one set of storms
            Now process it for QA/QC and finding Safir-Simpson value """
            #thisStorm.nobs = len(thisStorm.segs)
            
                

""" For each storm : """
"""     Create segment shapefiles """

"""     Create track shapefiles """

""" Write out shapefiles """