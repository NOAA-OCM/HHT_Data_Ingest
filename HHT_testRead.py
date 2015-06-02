# -*- coding: utf-8 -*-
"""
Reading and reformatting 3 files for HHT data reprocessing.
Python 3.4

Created 2014-12-11 by David L Eslinger (DLE)
    NOAA Office for Coastal Management
    Charleston, SC USA
    
Revised: 2014-12-18: HURDAT2 and IBTrACS import working for 2013 data (DLE)
         2015-04-16: Adding code to find Storm Report URLs from:
             http://www.nhc.noaa.gov/TCR_StormReportsIndex.xml (DLE)
         2015-05-28: Writing 4 shapefiles: goodTracks, goodSegemtns, 
             missingTracks and missingSegments.  The missing ones will be those
             with no valid pressure or wind speed records.  Most of those are 
             from the older storms.
         2015-06-02: Randomizing segment order to make tool behave as 
             previously with SQL process with segements in random order.
"""
import math
import random
import shapefile
import datetime as dt
import ensoDownload
import stormReportDownload
""" Declarations and Parameters """
SCRAMBLE = True
WEBMERC = False
TESTING = True
""" Choose to use either HURDAT2 data as the 'base' data layer (a new
    behaviour) or to use IBTrACS as the 'base' depending on the 
    use_HURDAT variable: """
use_HURDAT = True
dupRange = 5  

"""---------------------DEFINE WORKING DIRECTORIES------------------------"""
#workDir = "C:/GIS/Hurricane/HHT_Python/" # On OCM Work Machine
workDir = "N:/nac1/crs/deslinge/Data/Hurricane/" # On OCM Network
workDir = "/csc/nac1/crs/deslinge/Data/Hurricane/" # On OCM Linux
#workDir = "T:/DaveE/HHT/" # TEMP drive On OCM Network
#workDir = "/san1/tmp/DaveE/HHT/" # Temp drive On OCM Linux
#workDir = "/home/dave/Data/Hurricanes/" # On Zog
dataDir = workDir + "Data/"  # Data location
if TESTING:  
    h2nepacRaw = dataDir + "h2nmid.txt" # HURDAT2 NE North Pacific Data
    h2AtlRaw = dataDir + "h2mid.txt"     # HURDAT2 North Atlantic Data
    ibRaw = dataDir + "midAllcsc.csv"           # IBTrACS CSC version Data
#==============================================================================
#     h2nepacRaw = workDir + "h2NEPACtail.txt" # HURDAT2 NE North Pacific Data
#     h2AtlRaw = workDir + "h2ATLtail.txt"     # HURDAT2 North Atlantic Data
#     ibRaw = workDir + "IBtail200.csv"           # IBTrACS CSC version Data
#==============================================================================
    resultsDir = workDir + "Results/TRand1/"  #  Location for final data
else:
    #h2AtlRaw = dataDir + "hurdat2-atlantic-1851-2012-060513.txt"     # HURDAT2 North Atlantic Data
    #h2nepacRaw = dataDir + "hurdat2-nencpac-1949-2013-070714.txt" # HURDAT2 NE North Pacific Data
    #h2AtlRaw = dataDir + "hurdat2-1851-2013-052714.txt"     # HURDAT2 North Atlantic Data 2013
    h2AtlRaw = dataDir + "hurdat2-1851-2014-022315.txt"     # HURDAT2 North Atlantic Data 2014
    h2nepacRaw = dataDir + "hurdat2-nencpac-1949-2013-070714.txt" # HURDAT2 NE North Pacific Data
#    ibRaw = dataDir + "Allstorms.ibtracs_all.v03r06.csv" # IBTrACS ALL v03R06
    ibRaw = dataDir + "Allstorms.ibtracs_csc.v03r06.csv" # IBTrACS CSC v03R06
#    ibRaw = dataDir + "Allstorms.ibtracs_all.v03r05.csv" # IBTrACS ALL V03R05

    resultsDir = workDir + "Results/FullGeo02/"  #  Location for final data

"""--------------------------------------------------------------------"""
if WEBMERC:
    earthRadius = 6378137.0
    earthCircumference = math.pi * 2.0 * earthRadius  
    """ Define EPSG:3857 -- WGS84 Web Mercator (Auxiliary Sphere) Projection string 
        http://spatialreference.org/ref/sr-org/7483/ """
    epsg = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs'
    """ ESRI prj file below """
    #epsg = 'PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Mercator"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1]]'

    """--------------------------------------------------------------------"""
else:    
    """ Define WGS84 Geographic Projection string """
    epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
    """--------------------------------------------------------------------"""
  

""" Get data for ENSO stage for each segment by referencing year and
 month against data set at:
 http://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt
 For more information on the ENSO index, check out the CPC pages at:
 http://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ensoyears.shtml
 http://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_change.shtml
         """
""" Get dictionary of ENSO state by YYYY-MM key """
ensoLookup = {}
ensoLookup = ensoDownload.ensoDict()
""" Get NHC Storm reports for HURDAT storms from:
             http://www.nhc.noaa.gov/TCR_StormReportsIndex.xml (DLE)
"""
rptLookup = {}
rptLookup = stormReportDownload.rptDict()
Missing=[None, None]
#==============================================================================
# checkYears = ['2009','2010', '2011','2012']
# checkMonths = ['01','02','03','04','05','06','07','08','09','10','11','12']
# print(' Month: %s' % (checkMonths),end='')
# for year in checkYears:
#     print('\n  %s:' % (year),end='')
#     for month in checkMonths:
#         print(' %4s ' % (ensoLookup[year+'-'+month]),end='')
# print('\n\n')
#==============================================================================
""" Processing functions """
"""--------------------------------------------------------------------"""
def getStormReport(name,year):
    return "no report"
"""--------------------------------------------------------------------"""
def getCat(nature, wind):
    """ This function returns the appropriate classification of 
    Saffir-Simpson scale or other classification given the reported
    Nature and 1-minute averaged wind speed in nautical miles/hour (Knots).
    The logic used in the previous SQL calculations for classification was:
        DESCRIP_NAME HHT_CODE   MIN  MAX    COLOR       LINE     ?   ORIGINAL_NATURE
        Disturbance      DS      30   70    black       Solid    4      DB
        Extratropical    ET       0   0     black       dashed   5      EX
        Category 1       H1      64   83    red         Solid   10      HU
        Category 2       H2      83   96    red         Solid   11      HU
        Category 3       H3      96  113    dark red    Solid   12      HU
        Category 4       H4     113  137    dark red    Solid   13      HU
        Category 5       H5     137  999    dark red    Solid   14      HU
        Mixed Reports    MX      30   70    gray        Solid    3      NA or MX
        Unknown          N/A     -1  999    gray        Solid    2      NA
        N/A              NR      30   70    blue        Solid    1      NA
        Subtrop Depr     SD       0   34    orange      Solid    6      SD
        Subtrop Storm    SS      34  999    blue        Solid    7      SS
        Trop Depression  TD       0   34    green       Solid    8      TD
        Tropical Storm   TS      34   64    yellow      Solid    9      TS
    NOTE: As of 4/29/2015, the IBTrACS and HURDAT2 data files used a total
       of 13 different Nature names:
 'DB', 'DS', 'ET', 'EX', 'HU', 'LO', 'MX', 'NR', 'PT', 'SD', 'SS', 'TD', 'TS'
 IBTrACS uses just these in v03r06: 
 ['DS', 'ET', 'MX', 'NR', 'SS', 'TS']
 HURDAT2014/2015 uses:
 ['DB', 'ET', 'EX', 'HU', 'LO', 'PT', 'SD', 'SS', 'TD', 'TS', 'TY', 'WV']

    Boundary values and naming conventions used here follow the FAQ from
    NOAA's Hurricane Research Division:
            http://www.aoml.noaa.gov/hrd/tcfaq/A5.html
    and the Saffir-Simpson values as revised by the National Hurricane Center
    and defined in this document:
            http://www.nhc.noaa.gov/pdf/sshws_2012rev.pdf
    
    NOTE BENE: In addition, we have extended the UPPER boundary defined
    by the SS Scale up to, but not including, the lower boundary of the 
    next higher class.  This is necessary because the NHC's Saffir-Simpson 
    definitions are stricly for integer values of wind speed, which makes
    sense given the lack of precision at which they can accurately be 
    measured.  HOWEVER, when converting from units and averaging intervals 
    from other reporting Centers, there may be convereted 1-minute winds that 
    fall within the 1 knot "gaps" in the NHC's Saffir-Simpson definations. 
    In order to classify those wind speeds, we use the use the following
    logic to assign a storm to some class X:
        Lower Bound(Class X) <= Converted 1-min Wind < Upper Bound(Class X+1)
        
    Questions can be directed to:
        Dave Eslinger, dave.eslinger@noaa.gov
        
    """
    
    if wind >= 137:
        catSuffix = 'H5'
    elif wind >= 113:
        catSuffix = 'H4'
    elif wind >= 96:
        catSuffix = 'H3'
    elif wind >= 83:
        catSuffix = 'H2'
    elif wind >= 64:
        catSuffix = 'H1'
    elif wind >= 34:
        catSuffix = 'S' # Storm
    elif wind >= 0:
        catSuffix = 'D' # Depression
    else:
        return "NR"
        
    """ Now figure out what it is """
    if (catSuffix[0] == 'H' and (nature[0] == 'H' or nature[0] == 'T' 
        or nature == 'NR' or nature == 'MX' )):
        return catSuffix # It is a Hurricane strength and not extra-tropical
    elif (nature[0] == 'E'):
        return 'ET'
    elif (nature[0] == 'T' or nature[0] == 'P'):
        return 'T'+catSuffix
    elif (nature[0] == 'S'):
        return 'S'+catSuffix
    elif (nature == 'DS'): # Needed for IBTrACS Tropical Depressions
        return 'TD'        
    elif (nature == 'DB' or nature == 'LO' or nature == 'WV'):
        return 'DS' 
    elif (nature[0] == 'N' or nature[0] == 'M' or nature == 'NR'):
        return 'NR'
    else:
        print('ERROR in logic, Nature, wind, suffix = ',nature,wind, catSuffix)
        return 'Error_' + catSuffix
"""------------------------END OF getCat-------------------------------"""

""" Create needed Objects """
class Storm(object):
    def __init__(self,uid,name):
        self.uid = uid.strip()
        self.name = name.strip()
        self.startTime = None
        self.endTime = None
        self.maxW = float(-1.)
        self.minP = float(9999.)
        self.numSegs = 0
        self.maxSaffir = "NR"
        self.enso = ""
        self.source = ""  # 0 = IBTrACS, 1 or 2 = HURDAT2 Atl, NEPAC
        self.segs = []

class Observation(object):
    def __init__(self,time,lat,lon,wsp,pres,nature):
#==============================================================================
#         self.time = time.strip()
#         print(self.time)
#==============================================================================
        self.time = dt.datetime.strptime(time,'%Y-%m-%d %H:%M:%S')
        self.startLat = float(lat)
        self.startLon = float(lon)
        if float(wsp) <= 0:
            self.wsp = float(-1.0)
        else:
            self.wsp = float(wsp)
        if float(pres) <= 800.:
            self.pres = float(-1.0)
        else:
            self.pres = float(pres)
        self.nature = nature.strip()

class Segment(Observation):
    def __init__(self,time,lat,lon,wsp,pres,nature):
        super().__init__(time,lat,lon,wsp,pres,nature)
        self.endLat = float(0.)
        self.endLon = float(0.)
        self.saffir = ""
        self.enso = None

""" Create an empty list to hold allStorms
    and initialize the total storm counter """
allStorms = []
numStorms = -1
numAllMissing = 0
numSinglePoint = 0
numGoodObs = 0

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
print ('IBTrACS file: ', ibRaw)    
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
#     observation = Segment(vals[6],  # ISO 8601 Time 
     observation = Segment(vals[6],  # ISO 8601 Time 
                           vals[8],  # Lat
                           vals[9],  # Lon
                           vals[10], # Wind speed
                           vals[11], # Pressure
                           vals[7] ) # Nature
     thisStorm.segs.append(observation)
     thisStorm.startTime = observation.time
     thisStorm.name = thisStorm.name + " " \
         + thisStorm.startTime.strftime('%Y') 
     # enter end time in case this is only observation.
     thisStorm.endTime = observation.time 
     print(thisStorm.startTime)
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
                 thisStorm.name = thisStorm.name + " " \
                     + thisStorm.startTime.strftime('%Y') 
                 thisStorm.endTime = observation.time 
                 nseg = 1 # New storm ready for next record
                 thisStorm.source = 0 # Flag data source as IBTrACS
     """ EOF found on IBTrACS: Write last data and close out """           
     thisStorm.numSegs = len(thisStorm.segs)
     """ Only keep the storm if there is more than ONE observation: """
     if(thisStorm.numSegs > 1):
         allStorms.append(thisStorm) # Add old storm to allStorms
     else:
         numSinglePoint += 1
         
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
#hFiles = []
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
            thisStorm.numSegs =  int(vals[2])    # Number of Observations
            thisStorm.source = i + 1 # Flag data source as HURDAT ATL or NEPAC

#            print(thisStorm.uid, thisStorm.name, thisStorm.numSegs)

            for ob in range(thisStorm.numSegs):
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

                lat = (float(vals[4][:4]) if vals[4][4] == "N" 
                         else -1. * float(vals[4][:4]))
                lon = (float(vals[5][:5]) if vals[5][5] == "E" 
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
#                     thisStorm.numSegs, "observations and is index ", numStorms)            
#==============================================================================
            thisStorm.startTime = thisStorm.segs[0].time
            #thisStorm.name = thisStorm.name +" "+ thisStorm.startTime[:4]
            thisStorm.name = thisStorm.name + " " \
                 + thisStorm.startTime.strftime('%Y') 
            thisStorm.endTime = thisStorm.segs[len(thisStorm.segs)-1].time
            """ Only keep the storm if there is more than ONE observation: """
            if(thisStorm.numSegs > 1):
                 allStorms.append(thisStorm) # Add old storm to allStorms
            else:
                 numSinglePoint += 1
#==============================================================================
#             print ("Storm number ", len(allStorms)," named ",
#                    allStorms[numStorms].name,"has ", 
#                    len(allStorms[numStorms].segs), allStorms[numStorms].numSegs)
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
# for storm in allSorted:
#     if storm.startTime.find('2013') > -1:
#         print("SORTED: Source, UID, Name, Time, source = ", 
#          storm.source, storm.uid, storm.name, storm.segs[0].time)
#==============================================================================

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
        if we add the results, we should get '-2' for no duplicates"""
#==============================================================================
#         sameName = ( allSorted[i].name.find(allStorms[j].name)
#                  + allStorms[j].name.find(allSorted[i].name)  )
#==============================================================================
        """ Check names, but omit the year from the search string"""
        AsortedName = allSorted[i].name[:len(allSorted[i].name)-5]  
        AallName =  allStorms[j].name[:len(allStorms[j].name)-5]   
        AXsortedName = allSorted[i].name[:len(allSorted[i].name)-0]  
        AXallName =  allStorms[j].name[:len(allStorms[j].name)-0]   
        AallInSorted = AsortedName.find(AallName)
        AsortedInAll =  AallName.find(AsortedName)  
        if AallInSorted + AsortedInAll != -2: # Duplicate names found
            if ( allSorted[i].name.find("NAME") != -1 or 
                 allSorted[i].name.find("KNOWN") != -1 ):
                 """ Unnamed storms are common so check for identical
                     start times.  If different, they are different 
                     storms so CONTINUE to next j loop """
                 if allSorted[i].startTime != allStorms[j].startTime:
                     continue #different start time, not true duplicates
            nDups += 1
            isDuplicate = True
            dupIndex = j
            break
    
    if isDuplicate:
#==============================================================================
#         print ('\n', nDups, 'sets of duplicate storms found! \n',
#                'Source, Name, Start Date \n',
#                allSorted[i].source, allSorted[i].name,
#                allSorted[i].startTime, 'and \n',
#                allStorms[dupIndex].source,allStorms[dupIndex].name,
#                allStorms[dupIndex].startTime)
#==============================================================================
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
 
print ("\nIBTrACS: {0}, H2_ATL: {1}, H2_NEPAC: {2}".format(
        ibNum, hstormNum[0], hstormNum[1]), "\n ",
        "Total storms = {0}, Unique storms = {1}".format(
        len(allSorted),len(allStorms)), 
        "\nDuplicated storms: {0}".format(nDups,"" ))#nDup2s))

""" -------------------- All storms are now unique -------------------- """

""" Now process unique storms for QA/QC and finding Saffir-Simpson value """
""" Make a list of all the Nature types. Needed for setting up enso logic"""
allNatures = []
#for i, storm in enumerate(allStorms[11700:11802:4]):
#for i, storm in enumerate(allStorms[1:3]):
for i, storm in enumerate(allStorms):
    """loop through segments, skipping last"""
    storm.numSegs = len(storm.segs)
    jLast = storm.numSegs-1
    for j in range(0,jLast):
        """ For each segment in the storm find: """
        allNatures.append(storm.segs[j].nature)
        """ --- ending Lat and Lon for each segment"""
        storm.segs[j].endLat = storm.segs[j+1].startLat
        """ Make sure LONGITUDE do not change sign across the +-180 line"""
        if abs(storm.segs[j].startLon - storm.segs[j+1].startLon) > 270.:
            """ Lon crosses 180, so adjust all following
            startLons so it does not """
            storm.segs[j+1].startLon = (
                math.copysign(360.0,storm.segs[j].startLon)
                + storm.segs[j+1].startLon)
        storm.segs[j].endLon = storm.segs[j+1].startLon
        """ --- Saffir-Simpson value for each segment"""
        storm.segs[j].saffir = getCat(storm.segs[j].nature, 
                                    (storm.segs[j].wsp))

        """ Get data for ENSO stage for each segment by start time """
       # thisKey = storm.segs[j].time[:7]
        thisKey = storm.segs[j].time.strftime('%Y-%m')
        storm.segs[j].enso = ensoLookup.get(thisKey) 
        #print(thisKey, ensoLookup.get(thisKey),storm.segs[j].enso)          
        """ Find Max Winds and Saffir-Simpson and Min Pressures """
        if storm.segs[j].wsp > storm.maxW: # New Max found so update MaxW and SS
            storm.maxW = storm.segs[j].wsp
            storm.maxSaffir = storm.segs[j].saffir
        if storm.segs[j].pres < storm.minP and storm.segs[j].pres > 0:
            storm.minP = storm.segs[j].pres

    """ Now need to process the very last segment """ 
    """ --- ending Lat and Lon for each segment is just the same
    starting location, but offset by 0.01 degrees.  
    This allows for the creation of a valid attributed line for every
    actual observation."""   
    storm.segs[jLast].endLat = float(storm.segs[jLast].startLat + 0.01)
    storm.segs[jLast].endLon = float(storm.segs[jLast].startLon + 0.01)
   
    """ --- Saffir-Simpson value for each segment"""
    storm.segs[jLast].saffir = getCat(storm.segs[jLast].nature, 
                                (storm.segs[jLast].wsp))
    """ Find Max Winds and Saffir-Simpson and Min Pressures """
    if storm.segs[jLast].wsp > storm.maxW: # New Max found so update MaxW and SS
        storm.maxW = storm.segs[jLast].wsp
        storm.maxSaffir = storm.segs[jLast].saffir
    if storm.segs[jLast].pres < storm.minP and storm.segs[jLast].pres > 0:
        storm.minP = storm.segs[jLast].pres
    """ Get data for ENSO stage for last segment by start time """
    try:
        thisKey = storm.segs[jLast].time.strftime('%Y-%m')
    except:
        print(j, storm.segs[jLast].time)
    #thisKey = storm.segs[jLast].time[:7]
    storm.segs[jLast].enso = ensoLookup.get(thisKey) 
#    storm.segs[jLast].enso = ensoLookup[storm.segs[jLast].time[:7]]           

    """ If Maximum Wind and Minimum Pressure are still the inital values, 
    replace them with MISSING VALUE FLAGS """
    if storm.maxW == -99.:
        storm.maxW = "-1.0"
    if storm.minP == 9999.:
        storm.minP = "-1.0"
        
#==============================================================================
# uniqueNatures = set(allNatures)
# print(sorted(uniqueNatures))
#==============================================================================

stormFields = [['STORMID','C','56'],
               ['MxWind1min','N','19'],
               ['BASIN','C','4'],
               ['Disp_Name','C','81'],
               ['DDateRange','C','140'],
               ['BegObDate','D','10'],
               ['EndObDate','D','10'],
               ['D_SaffirS','C','10'],
               ['FP_Years','C','10'],
               ['FP_Months','C','10'],
               ['FP_CR','C','10'],
               ['FP_MSS','C','10'],
               ['FP_MP','N','10'],
               ['StrmRptURL','C','254'],
               ['In10sOrder','N','10'], # End of Previous Attributes
               ['NumObs','C','10'],
               ['ENSO','C','10']]
#==============================================================================
# stormFields = ['UID','Name','StartDate','EndDate','MaxWind','MinPress',
#                'NumObs','MaxSaffir','ENSO']
#==============================================================================
""" Create and initalize the fields for the needed Tracks Shapefiles """
goodTracks = shapefile.Writer(shapefile.POLYLINE) #One line & record per storm
goodTracks.autobalance = 1 # make sure all shapes have records
missingTracks = shapefile.Writer(shapefile.POLYLINE) #One line & record per storm
missingTracks.autobalance = 1 # make sure all shapes have records
for attribute in stormFields:
    goodTracks.field(attribute[0],attribute[1],attribute[2]) # Add Fields
    missingTracks.field(attribute[0],attribute[1],attribute[2]) # Add Fields
     

""" For SEGMENTS : """
segmentFields = [['STORMID','C','58'],
                 ['MSW_1min','N','9'],
                 ['BeginObHr','N','9'],
                 ['BeginLat','C','10'],
                 ['BEGINLON','C','10'],
                 ['Min_Press','C','10'],
                 ['Basin','C','10'],
                 ['SS_Scale','C','10'],
                 ['DateNTime','C','20'],
                 ['DMSW_1min','C','10'],
                 ['DispName','C','150'],
                 ['DispDate','C','20'],
                 ['DMin_Press','C','10'],
                 ['DDateNTime','C','20'], #End of previous attributes
                 ['Nature','C','20'],
                 ['ENSO','C','20'],
                 ['EndLat','C','20'],
                 ['EndLon','C','20']]               

""" Create and initalize the fields for the needed Tracks Shapefiles """
goodSegments = shapefile.Writer(shapefile.POLYLINE) # New shapefile
goodSegments.autoBalance = 1 # make sure all shapes have records
missingSegments = shapefile.Writer(shapefile.POLYLINE) # New shapefile
missingSegments.autoBalance = 1 # make sure all shapes have records
for attribute in segmentFields: # Add Fields for track shapefile
   # print(attribute)
    goodSegments.field(attribute[0],attribute[1],attribute[2]) 
    missingSegments.field(attribute[0],attribute[1],attribute[2]) 
"""Lists needed for SCRAMBLING Segemnts """
goodSegCoords = []
goodSegParams = []
goodSegNum = 0
goodSegIndx = []

missingSegCoords = []
missingSegParams = []
missingSegNum = 0
missingSegIndx= []
       
for i, storm in enumerate(allStorms):
                
    lineCoords = [] # Create list for stormTracks shapefile
    """ Find out if this hurricane has any valid pressure or wind speed obs """
    if (float(storm.minP) < 0.0 and float(storm.maxW) < 0.0 ):
        goodStorm = False
    else:
        goodStorm = True
    for thisSegment in storm.segs:
        """ Project to web mercator if need, otherwise just geographic"""
        if WEBMERC:
            sLon = earthRadius * thisSegment.startLon * math.pi/180
            sLat = earthRadius * math.log( 
                math.tan((math.pi/4) + ((thisSegment.startLat*math.pi/180)/2)))
            eLon = earthRadius * thisSegment.endLon * math.pi/180
            eLat = earthRadius * math.log( 
                math.tan((math.pi/4) + ((thisSegment.endLat*math.pi/180)/2)))
        else:
            sLon = thisSegment.startLon
            sLat = thisSegment.startLat
            eLon = thisSegment.endLon
            eLat = thisSegment.endLat
        """ Add coordinates to the Track shapefile list """
        lineCoords.append([[sLon, sLat],
                              [eLon, eLat]])
                
        """ We need to putput these attributes: 
        ['STORMID','MSW_1min','BeginObHr','BeginLat','BEGINLON',
                 'Min_Press',
                 'Basin','SS_Scale','DateNTime','DMSW_1min',
                 'DispName','DispDate','DMin_Press','DDateNTime', #End of previous attributes
                 'Nature','ENSO',
                 'EndLon','EndLat'] """
        """ Extra values to match old (pre-2015) database structure """
        basin = rptLookup.setdefault(storm.name,Missing)[1]
        begObsHour = dt.datetime.strftime(thisSegment.time,'%H%M')
        dateTime = dt.datetime.strftime(thisSegment.time,'%m/%d/%Y %H')
        dispDate = dt.datetime.strftime(thisSegment.time,'%b %d, %Y')
        dispDateTime = dt.datetime.strftime(thisSegment.time,'%b %d, %Y %Hz')
  
        """ Add this segment's data to the appropriate segments shapefile """
        if goodStorm:
#==============================================================================
#             goodSegments.poly(parts = [[[sLon, sLat],[eLon, eLat]]])
#             goodSegments.record(storm.uid,           # Storm ID
#==============================================================================
            goodSegCoords.append([[sLon, sLat],[eLon, eLat]])
            goodSegParams.append([storm.uid,           # Storm ID
                           thisSegment.wsp,     # Max. Sustained Wind
                           begObsHour,          # Begin Observation Hour Why?
                           thisSegment.startLat,# Begin Lat
                           thisSegment.startLon,# Begin Long.
                           thisSegment.pres,    # Min Pressure
                           basin,               # Basin
                           thisSegment.saffir,  # Saffir Simpson Scale
                           dateTime,            # Date and Time
                           thisSegment.wsp,     # Display Max. Sustained Wind
                           storm.name,          # Display Storm Name
                           dispDate,            # Display Date
                           thisSegment.pres,    # Display Min Pressure
                           dispDateTime,        # Display Date and Time
                           # End of Previous Attributes
                           thisSegment.nature,  # Nature (not quite SS)
                           thisSegment.enso,    # ENSO Flag
                           thisSegment.endLat,  # End Lat
                           thisSegment.endLon] )  # End Long.                          
            goodSegIndx.append(goodSegNum)
            goodSegNum += 1
                    
        else:
#==============================================================================
#             missingSegments.poly(parts = [[[sLon, sLat],[eLon, eLat]]])
#             missingSegments.record(storm.uid,           # Storm ID
#==============================================================================
            missingSegCoords.append([[sLon, sLat],[eLon, eLat]])
            missingSegParams.append([storm.uid,           # Storm ID
                            thisSegment.wsp,     # Max. Sustained Wind
                           begObsHour,          # Begin Observation Hour Why?
                           thisSegment.startLat,# Begin Lat
                           thisSegment.startLon,# Begin Long.
                           thisSegment.pres,    # Min Pressure
                           basin,               # Basin
                           thisSegment.saffir,  # Saffir Simpson Scale
                           dateTime,            # Date and Time
                           thisSegment.wsp,     # Display Max. Sustained Wind
                           storm.name,          # Display Storm Name
                           dispDate,            # Display Date
                           thisSegment.pres,    # Display Min Pressure
                           dispDateTime,        # Display Date and Time
                           # End of Previous Attributes
                           thisSegment.nature,  # Nature (not quite SS)
                           thisSegment.enso,    # ENSO Flag
                           thisSegment.endLat,  # End Lat
                           thisSegment.endLon] )  # End Long.
            missingSegIndx.append(missingSegNum)
            missingSegNum += 1

#==============================================================================
#     print(lineCoords)
#     foo = input(" any key to continue")
#==============================================================================
    """ Find ENSO state for start of the storm """
    storm.enso = ensoLookup.get(storm.segs[0].time.strftime('%Y-%m'))
    """ Need to output these fields:
            'STORMID','MxWind1min','BASIN','Disp_Name','DDateRange',
               'BegObDate','EndObDate','D_SaffirS',
               'FP_Years','FP_Months','FP_CR','FP_MSS','FP_MP',
               'StrmRptURL','In10sOrder' # End of Previous Attributes
               'NumObs','ENSO']"""
    """ Extra values to match old (pre-2015) database structure """
    basin = rptLookup.setdefault(storm.name,Missing)[1]
    rptURL = rptLookup.setdefault(storm.name,Missing)[0]
    strmStart = storm.segs[0].time
    strmEnd = storm.segs[len(storm.segs)-1].time
    
    dateRng = dt.datetime.strftime(strmStart,'%b %d, %Y to ') + \
              dt.datetime.strftime(strmEnd,'%b %d, %Y')
    yr1 = strmStart.year
    yr2 = strmEnd.year
    filtYrs = str(strmStart.year)
    if yr1 < yr2:
       # print('Storm ',storm.name,' spans years')
        for iyr in range(strmStart.year + 1, strmEnd.year+1):
            filtYrs = filtYrs + ', %d' %iyr
    #filtYrs = 
    filtMons = str(strmStart.month)
    if strmEnd.month < strmStart.month: 
        """ Storm spans years, so need all months from start through December, 
            then from January through end."""
        for imnth in range(strmStart.month+1,13):
             filtMons = filtMons + ', %d' %imnth
        for imnth in range(1,strmEnd.month+1):
             filtMons = filtMons + ', %d' %imnth
    else:
        """ For storms within one year, list consecutive months. """
        for imnth in range(strmStart.month+1,strmEnd.month+1):
             filtMons = filtMons + ', %d' %imnth
        
    intensOrder = 0
    filtClimReg = ""
    """   --------  End of Extra fields   ------------    """
    """ Append track to appropriate stormTracks list """
    if goodStorm:
        numGoodObs += 1
        goodTracks.poly(shapeType=3, parts = lineCoords ) # Add the shape
        goodTracks.record(storm.uid,       # StormID
                       storm.maxW,      # Max Sustained WInd, 1 min ave period
                       basin,           # Basin
                       storm.name,      # Display Storm Name
                       dateRng,         # Display Date Range
                       storm.startTime, # Begin Observation Date
                       storm.endTime,   # End Observation Date
                       storm.maxSaffir, # Display Saffir Simpson
                       filtYrs,         # Filter Param. Years
                       filtMons,        # Filter Param. Months
                       filtClimReg,     # Filter Param. Climate Regions
                       storm.maxSaffir, # Filter Param. Saffir Simpson 2 letter
                       storm.minP,      # Filter Param: Minimum Pressure
                       rptURL,          # Storm Report URL
                       intensOrder,        # Intensity Order (numeric)
                       # Extra Attributes below
                       storm.numSegs,   # Number of segments in this Track
                       storm.enso)      # ENSO Flag
    else:
        numAllMissing += 1
        missingTracks.poly(shapeType=3, parts = lineCoords ) # Add the shape
        missingTracks.record(storm.uid,       # StormID
                       storm.maxW,      # Max Sustained WInd, 1 min ave period
                       basin,           # Basin
                       storm.name,      # Display Storm Name
                       dateRng,         # Display Date Range
                       storm.startTime, # Begin Observation Date
                       storm.endTime,   # End Observation Date
                       storm.maxSaffir, # Display Saffir Simpson
                       filtYrs,         # Filter Param. Years
                       filtMons,        # Filter Param. Months
                       filtClimReg,     # Filter Param. Climate Regions
                       storm.maxSaffir, # Filter Param. Saffir Simpson 2 letter
                       storm.minP,      # Filter Param: Minimum Pressure
                       rptURL,          # Storm Report URL
                       intensOrder,        # Intensity Order (numeric)
                       # Extra Attributes below
                       storm.numSegs,   # Number of segments in this Track
                       storm.enso)      # ENSO Flag
""" All done, so scramble Segments if needed.  
    Then populate Segments shapefile"""
if (SCRAMBLE):
    random.shuffle(goodSegIndx)
    random.shuffle(missingSegIndx)   
for i in goodSegIndx:
    print(goodSegCoords[i])
    goodSegments.poly(parts = goodSegCoords[i])
    goodSegments.record(goodSegParams[i])

for i in missingSegIndx:
    missingSegments.poly(parts = missingSegCoords[i])
    missingSegments.record(missingSegParams[i])

        
""" Save shapefile """
#    thisName = resultsDir+storm.name.replace(":","_")+"_"+storm.startTime[:4]
if WEBMERC:
    goodSegmentName = resultsDir+'goodSegments_WebMerc_2015'
    goodStormFileName = resultsDir+'goodTracks_WebMerc_2015'
    missingSegmentName = resultsDir+'missingSegments_WebMerc_2015'
    missingStormFileName = resultsDir+'missingTracks_WebMerc_2015'
else:
     goodSegmentName = resultsDir+'goodSegments_2015'
     goodStormFileName = resultsDir+'goodTracks_2015'
     missingSegmentName = resultsDir+'missingSegments_2015'
     missingStormFileName = resultsDir+'missingTracks_2015'
#print("goodSegments.shapeType = ",goodSegments.shapeType)

goodSegments.save(goodSegmentName)
# create the PRJ file
prj1 = open("%s.prj" % goodSegmentName, "w")
prj1.write(epsg)
prj1.close()   

missingSegments.save(missingSegmentName)
prj2 = open("%s.prj" % missingSegmentName, "w")
prj2.write(epsg)
prj2.close()   

goodTracks.save(goodStormFileName)
prj3 = open("%s.prj" % goodStormFileName, "w")
prj3.write(epsg)
prj3.close()

missingTracks.save(missingStormFileName)
# create the PRJ file
prj4 = open("%s.prj" % missingStormFileName, "w")
prj4.write(epsg)
prj4.close()

print(' Valid tracks: ',numGoodObs, '\n No Valid Obs: ',numAllMissing, 
      '\n Single Point storms: ', numSinglePoint)
