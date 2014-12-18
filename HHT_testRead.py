# -*- coding: utf-8 -*-
"""
Reading and reformatting 3 files for HHT data reprocessing.
Python 3.4

Created 2014-12-11 by David L Eslinger (DLE)
    NOAA Office for Coastal Management
    Charleston, SC USA
    
Revised: 2014-12-18: HURDAT2 and IBTrACS import working for 2013 data (DLE)

"""
#import csv

""" Declarations and Parameters """
#workDir = "C:/GIS/Hurricane/HHT_Python/" # On Work Machine
workDir = "/home/dave/Data/Hurricanes/" # On ZOG
dataDir = workDir  # Main Data location
h2AtlRaw = dataDir + "h2ATL.txt"     # HURDAT2 North Atlantic Data
h2nepacRaw = dataDir + "h2NEPAC.txt" # HURDAT2 NE North Pacific Data
ibRaw = dataDir + "IBx.csv"           # IBTrACS CSC version Data

resultsDir = workDir + "Results/"  #  Location for final data

""" Create needed Objects """
class Storm(object):
    def __init__(self,uid,name):
        self.uid = uid
        self.name = name
        self.maxW = -99
        self.minP = 999
        self.numSegs = 0
        self.segs = []
        self.startTime = None
        self.endTime = None
        self.maxSafir = ""

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
                observation = Segment(otime,     # ISO Time
                                      lat,       # Latitude
                                      lon,       # Longitude
                                      vals[6],   # Wind Speed
                                      vals[7],   # Air Pressure
                                      vals[3] )  # Nature
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
ibNum = 0 # Initialize IBTrACS storm counter, it will increment when storm end is found
    
with open(ibRaw, "r") as rawObsFile:
     head1 = rawObsFile.readline()
     head2 = rawObsFile.readline()
     head3 = rawObsFile.readline()
#     print(head1, head2, head3)
     """ Read first IBTrACS Record """
     lineVals = rawObsFile.readline() # First Storm record in IBTrACS
     vals = lineVals.split(",")

     thisStorm = Storm(vals[0], vals[5]) # Create first storm
     observation = Segment(vals[6],  # ISO time 
                           vals[8],  # Lat
                           vals[9],  # Lon
                           vals[10], # Wind speed
                           vals[11], # Pressure
                           vals[7] ) # Nature
     thisStorm.segs.append(observation)
     nseg = 1
     """ First storm and observation entered, begin looping """
     
     """ NOTE BENE:  Really need to find the first storm that 
     is not in the IBTRaCS data"""
     
     while True: # With this and the below break, read to EOF
         lineVals = rawObsFile.readline()
         if not lineVals: # Finds EOF
             break # Break on EOF
         else: # Data read: Parse it and test to see if it is a new storm
             vals = lineVals.split(",")
             if vals[0] == thisStorm.uid :  # Same storm so add the record
                 observation = Segment(vals[6],  # ISO time 
                                       vals[8],  # Lat
                                       vals[9],  # Lon
                                       vals[10], # Wind speed
                                       vals[11], # Pressure
                                       vals[7] ) # Nature
                 thisStorm.segs.append(observation)
                 nseg += 1
             else: #Found a new storm so...
                 thisStorm.numSegs = len(thisStorm.segs)
                 allStorms.append(thisStorm) # Add old storm to allStorms
                 ibNum += 1 # Increment counter for IBTrACS storms
                 print("IBTrACS storm # ",ibNum," named ",thisStorm.name,
                       " has ", thisStorm.numSegs," observations \n    which ",
                       "should be ", nseg)
                 
                 """ Create a new storm record for the newly read storm """
                 thisStorm = Storm(vals[0], vals[5]) # Create next storm
                 observation = Segment(vals[6],  # ISO time 
                                       vals[8],  # Lat
                                       vals[9],  # Lon
                                       vals[10], # Wind speed
                                       vals[11], # Pressure
                                       vals[7] ) # Nature
                 thisStorm.segs.append(observation)
                 nseg = 1 # New storm ready for next record
     """ EOF found on IBTrACS: Write last data and close out """           
     thisStorm.numSegs = len(thisStorm.segs)
     allStorms.append(thisStorm) # Add old storm to allStorms
     ibNum += 1 # Increment counter for IBTrACS storms
     print("Last IBTrACS storm # ",ibNum," named ",thisStorm.name,
           " has ", thisStorm.numSegs," observations \n    which ",
           "should be ", nseg)

""" End of IBTrACS Ingest """

""" Now process all storms for QA/QC and finding Safir-Simpson value """
            
                

""" For each storm : """
"""     Create segment shapefiles """

"""     Create track shapefiles """

""" Write out shapefiles """