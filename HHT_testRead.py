# -*- coding: utf-8 -*-
"""
Reading and reformatting 3 files for HHT data reprocessing.
Python 3.4

Created 2014-12-11 by David L Eslinger (DLE)
    NOAA Office for Coastal Management
    Charleston, SC USA
    
Revised: 2014-12-18: HURDAT2 and IBTrACS import working for 2013 data (DLE)

"""

""" Declarations and Parameters """
workDir = "C:/GIS/Hurricane/HHT_Python/" # On Work Machine
#workDir = "/home/dave/Data/Hurricanes/" # On Zog
#==============================================================================
# dataDir = workDir  # Testing Data location
# h2nepacRaw = workDir + "h2NEPACtail.txt" # HURDAT2 NE North Pacific Data
# h2AtlRaw = workDir + "h2ATLtail.txt"     # HURDAT2 North Atlantic Data
# ibRaw = workDir + "IBtail200.csv"           # IBTrACS CSC version Data
#==============================================================================
h2_dataDir = "C:/GIS/Hurricane/HURDAT/"  # Main Data location
h2AtlRaw = h2_dataDir + "hurdat2-atlantic-1851-2012-060513.txt"     # HURDAT2 North Atlantic Data
h2nepacRaw = h2_dataDir + "hurdat2-nencpac-1949-2013-070714.txt" # HURDAT2 NE North Pacific Data
ib_dataDir = "C:/GIS/Hurricane/IBTrACS/v03r06/"  # Main Data location
ibRaw = ib_dataDir + "Allstorms.ibtracs_csc.v03r06.csv"           # IBTrACS CSC version Data
#ib_dataDir = "C:/GIS/Hurricane/IBTrACS/v03r04/"  # Main Data location
#ibRaw = ib_dataDir + "Allstorms.ibtracs_wmo.v03r04.csv"           # IBTrACS CSC version Data

resultsDir = workDir + "Results/"  #  Location for final data

""" Choose to use either HURDAT2 data as the 'base' data layer (a new
    behaviour) or to use IBTrACS as the 'base' depending on the 
    use_HURDAT variable: """
use_HURDAT = False
dupRange = 5    

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
        self.enso = ""
        self.source = ""  # 0 = IBTrACS, 1 or 2 = HURDAT2 Atl, NEPAC

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
        super().__init__(time,lat,lon,wsp,pres,nature)
        self.endLat = None
        self.endLon = None
        self.safir = None

""" Create an empty list to hold allStorms
    and initialize the total storm counter """
allStorms = []
numStorms = -1

""" Read IBTrACS data 
    This data is not split by storms, rather every row has all info in it
    Therefore, we must read the data, find out if it is a new storm, and
    if it is, then write the previous storm object/information to allStorms 
    and create a new thisStorm object and populate it with the first 
    observation.
    
    We know the IBTrACS data starts with 3 header rows, then the 4th row
    is our first legitimate data record.  
    Initialize the first thisStorm object from that"""
    
ibNum = 0 # Initialize IBTrACS storm counter, 
          # it will increment when storm end is found
    
with open(ibRaw, "r") as rawObsFile:
     head1 = rawObsFile.readline()
     head2 = rawObsFile.readline()
     head3 = rawObsFile.readline()
#     print(head1, head2, head3)
     """ Read first IBTrACS Record """
     lineVals = rawObsFile.readline() # First Storm record in IBTrACS
     vals = lineVals.split(",")
     """ Create first storm """
     thisStorm = Storm(vals[0],          # Unique IBTrACS ID
                       vals[5].strip())  # Name, spaces removed
     observation = Segment(vals[6],  # ISO 8601 Time 
                           vals[8],  # Lat
                           vals[9],  # Lon
                           vals[10], # Wind speed
                           vals[11], # Pressure
                           vals[7] ) # Nature
     thisStorm.segs.append(observation)
     thisStorm.startTime = observation.time
     # enter end time in case this is only observation.
     thisStorm.endTime = observation.time 
     nseg = 1
     thisStorm.source = 0            # Flag data source as IBTrACS
     """ First storm and observation entered, begin looping """
     while True: # With this and the below break, read to EOF
         lineVals = rawObsFile.readline()
         if not lineVals: # Finds EOF
             break # Break on EOF
         else: # Data read: Parse it and test to see if it is a new storm
             vals = lineVals.split(",")
             if vals[0] == thisStorm.uid :  # Same storm so add the record
                 observation = Segment(vals[6],  # ISO 8601 Time 
                                       vals[8],  # Lat
                                       vals[9],  # Lon
                                       vals[10], # Wind speed
                                       vals[11], # Pressure
                                       vals[7] ) # Nature
                 thisStorm.endTime = observation.time #update end time
                 thisStorm.segs.append(observation)
                 nseg += 1
             else: #Found a new storm so...
                 thisStorm.numSegs = len(thisStorm.segs)
                 allStorms.append(thisStorm) # Add old storm to allStorms
                 ibNum += 1 # Increment counter for IBTrACS storms
#==============================================================================
#                  print("IBTrACS storm # ",ibNum," named ",thisStorm.name,
#                        " has ", thisStorm.numSegs," observations \n    which ",
#                        "should be ", nseg)
#==============================================================================
                 
                 """ Create a new storm record for the newly read storm """
                 thisStorm = Storm(vals[0],          # Unique IBTrACS ID
                                   vals[5].strip())  # Name, spaces removed
                 """ Add the first segment information to the storm """
                 observation = Segment(vals[6],  # ISO 8601 Time 
                                       vals[8],  # Lat
                                       vals[9],  # Lon
                                       vals[10], # Wind speed
                                       vals[11], # Pressure
                                       vals[7] ) # Nature
                 thisStorm.segs.append(observation)
                 thisStorm.startTime = observation.time
                 # enter end time in case this is only observation.
                 thisStorm.endTime = observation.time 
                 nseg = 1 # New storm ready for next record
                 thisStorm.source = 0 # Flag data source as IBTrACS
     """ EOF found on IBTrACS: Write last data and close out """           
     thisStorm.numSegs = len(thisStorm.segs)
     allStorms.append(thisStorm) # Add old storm to allStorms
     ibNum += 1 # Increment counter for IBTrACS storms
#==============================================================================
#      print("Last IBTrACS storm # ",ibNum," named ",thisStorm.name,
#            " has ", thisStorm.numSegs," observations \n    which ",
#            "should be ", nseg)
#==============================================================================

""" End of IBTrACS Ingest """

""" Read HURDAT2 data """
     
""" NOTE BENE:  May prefer to find the first storm that 
    is not in the IBTRaCS data  """    

hFiles = [h2AtlRaw, h2nepacRaw]
hstormNum = [0,0]
#hFiles = [h2AtlRaw]
for i, file in enumerate(hFiles):
    print (i, file)
    hstormNum[i] = 0
    with open(file, "r") as rawObsFile:
        """h2reader = csv.reader(rawObsFile, delimiter=",")
        for row in h2reader:
            print(row)"""
        """ Need a manual loop here to read a header record then 
        the rest of the observations """   
        while True: # With this and the below break, read to EOF
            lineVals = rawObsFile.readline()
            if ( (not lineVals) # Finds EOF or any blank line
            or lineVals == "\n" or lineVals == "\r" or lineVals == "\n\r"): 
                break # Break on EOF

            """ This is a new storm so create a new storm record for it """
            numStorms += 1
            hstormNum[i] += 1
            vals = lineVals.split(",")
            #print ("vals = ",vals[0],vals[1],vals[2], len(vals))
            thisStorm = Storm(vals[0],  # Create new storm using Unique ID 
                              vals[1].strip())  # and Name w/out spaces
            thisStorm.nobs =  int(vals[2])    # Number of Observations
            thisStorm.source = i + 1 # Flag data source as HURDAT ATL or NEPAC

#            print(thisStorm.uid, thisStorm.name, thisStorm.nobs)

            for ob in range(thisStorm.nobs):
                lineVals = rawObsFile.readline()
                if ( (not lineVals) # Finds EOF or any blank line
                or lineVals == "\n" or lineVals == "\r" 
                or lineVals == "\n\r"): # lineVals is false at EOF
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
                observation = Segment(otime,     # ISO 8601 Time
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
#                     thisStorm.nobs, "observations and is index ", numStorms)            
#==============================================================================
            thisStorm.startTime = thisStorm.segs[0].time
            thisStorm.endTime = thisStorm.segs[len(thisStorm.segs)-1].time
            allStorms.append(thisStorm)
#==============================================================================
#             print ("Storm number ", len(allStorms)," named ",
#                    allStorms[numStorms].name,"has ", 
#                    len(allStorms[numStorms].segs), allStorms[numStorms].nobs)
#==============================================================================
""" End of HURDAT2 Ingest"""   

""" Sort combined storms and keep unique ones
    Use sotrm.source field to pick either HURDAT or IBTrACS storms
    based on value of use_HURDAT boolean """
    
allSorted = sorted(allStorms, key = lambda storm: storm.startTime)
#allSorted = sorted(allStorms, key = lambda storm: storm.name)
#==============================================================================
# for storm in allStorms:
#     print("Name, Time = ", storm.name, storm.segs[0].time)
#==============================================================================
for storm in allSorted:
    if storm.startTime.find('2013') > -1:
        print("SORTED: Source, UID, Name, Time, source = ", 
         storm.source, storm.uid, storm.name, storm.segs[0].time)

allStorms = [] # Clear allStorms variable to use for unique storms
allStorms.append(allSorted[0]) # Add first storm to the non-duplicate list
nDups = 0

for i in range(1,len(allSorted)):  # Cycle through all the Sorted storms
#    print('i =',i)
    """ Compare current Storm to dupRange of previously identified "good"
        storms in the AllStorms list """
    isDuplicate = False # intialize the flag for this storm
    dupIndex = None
    """ Iterate from end of allStorms, backward by dupRange records, or just
        to the length of allStorms, whichever is shortest """
    lastGood = len(allStorms)-1
    for j in range(lastGood,lastGood-min(dupRange, lastGood),-1 ):
        """ To find a duplicate name, use the .find method for stings.  
        This will return a value of '-1' if the string is not found.  
        NOTE: The IBtRACS names are frequently combinations of names 
        from multiple reporting Centers.  
        Therefore, we need to do this check for both 'directions' and 
        if we add the results, we shoudl get '-2' for no duplicates"""
#==============================================================================
#         sameName = ( allSorted[i].name.find(allStorms[j].name)
#                  + allStorms[j].name.find(allSorted[i].name)  )
#==============================================================================
        AsortedName = allSorted[i].name
        AallName =  allStorms[j].name  
        AallInSorted = AsortedName.find(AallName)
        AsortedInAll =  AallName.find(AsortedName)  
        if AallInSorted + AsortedInAll != -2: #Duplicate names found
            if ( allSorted[i].name.find("NAMED") != -1 and 
                 allSorted[i].name.find("UNKNOWN") != -1 ):
               pass #may not be duplicates
            else:
               nDups += 1
               isDuplicate = True
               dupIndex = j
               break
    
    if isDuplicate:
        print ('\n', nDups, 'sets of duplicate storms found! \n',
               'Source, Name, Start Date \n',
               allSorted[i].source, allSorted[i].name,
               allSorted[i].startTime, 'and \n',
               allStorms[dupIndex].source,allStorms[dupIndex].name,
               allStorms[dupIndex].startTime)
        if use_HURDAT:
             if allSorted[i].source: #This is a HURDAT record so replace old one
                 allStorms[dupIndex] = allSorted[i]
             else: # The existing allStorm record is HURDAT, so keep it
                 pass
        else: # Want to use IBTrACS for duplicates
            if allSorted[i].source: #The new record is HURDAT, so skip it
                pass
            else: # The existing allStorm record is HURDAT, so replace it
                allStorms[dupIndex] = allSorted[i]

    else: # not a duplicate, so copy it to allStorms
        allStorms.append(allSorted[i])
        if allSorted[i].source:
            print ("H2[{0}] Only Storm {1} from {2} to {3}".format(
            allSorted[i].source,allSorted[i].name, 
            allSorted[i].startTime, allSorted[i].endTime))
            
 
print ("\nIBTrACS: {0}, H2_ATL: {1}, H2_NEPAC: {2}".format(
        ibNum, hstormNum[0], hstormNum[1]), "\n ",
        "Total storms = {0}, Unique storms = {1}".format(
        len(allSorted),len(allStorms)), 
        "\nDuplicated storms: {0}, {1}".format(nDups,"" ))#nDup2s))

""" Now process unique storms for QA/QC and finding Safir-Simpson value """

            
                

""" For each storm : """
"""     Create segment shapefiles """

"""     Create track shapefiles """

""" Write out shapefiles """