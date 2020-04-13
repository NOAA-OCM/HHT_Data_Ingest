# -*- coding: utf-8 -*-
"""
Created on Sat Oct 12 15:26:41 2019

@author: Dave Eslinger
         dave.eslinger@noaa.gov
         2019-10-12: Not working yet

Processes HURDAT2 and IBTrACS data sets and formats for Historical Hurricane
Tracks web site.

New version using Pandas and modular approach for cloud deployment.

"""

""" Standard Python libraries  """
import os
import sys
#import pandas as pd
#import numpy as np
import math
import random
import json
import datetime as dt
import shapefile
#import geopandas as gpd
#from shapely.geometry import MultiLineString
import configparser

"""  These next two packages are custom packages for this progam.  They
    should be in the same directory as HHT_Annualupdate
    """
import loadENSODict # Local python module
import loadStormReportDict # Local python module

""" Declarations and Parameters from Configuration file"""
config = configparser.ConfigParser()
config.read('./config.ini')


SCRAMBLE = config.getboolean('PARAMETERS','SCRAMBLE')
WEBMERC = config.getboolean('PARAMETERS','WEBMERC')
BREAK180 = config.getboolean('PARAMETERS','BREAK180')
OMIT_PROVISIONAL = config.getboolean('PARAMETERS','OMIT_PROVISIONAL')
LABEL_PROVISIONAL = config.getboolean('PARAMETERS','LABEL_PROVISIONAL')
TESTING = config.getboolean('PARAMETERS','TESTING')
FLAG_BAD = config.getboolean('PARAMETERS','FLAG_BAD')

""" If NO391521 is True, then omit obs at 03:00, 09:00, 15:00 and 21:00 from IBTrACS.
    These appear to be poor quality (DLE's observation) records from different
    reporting groups and give the dashed black-colored zig zag look to many
    tracks in the Indian Ocean. """
NO391521 = config.getboolean('PARAMETERS','NO391521')

""" Choose to use either HURDAT2 data as the 'base' data layer (a new
    behaviour) or to use IBTrACS as the 'base' depending on the
    use_HURDAT variable: """
USE_HURDAT = config.getboolean('PARAMETERS','USE_HURDAT')

"""---------- DEFINE WORKING DIRECTORIES AND FILE NAMES --------------------"""
workDir = config.get('DIRECTORIES','WORKDIR')
dataDir = config.get('DIRECTORIES','DATA')
resultsDir = config.get('DIRECTORIES','RESULTS')
logDir = config.get('DIRECTORIES','RESULTS_LOG')

""" Create the needed Results and Logs directories if needed """
if( not os.path.isdir(resultsDir) ):
    try:
        os.makedirs(resultsDir, exist_ok=True)
    except:
        sys.exit("Creation of results directory failed")
    else:
        print("Results directory successfully created")
else:
    print("Results directory already exists")

if( not os.path.isdir(logDir) ):
    try:
        os.makedirs(logDir, exist_ok=True)
    except:
        sys.exit("Creation of results directory failed")
    else:
        print("Log directory successfully created")
else:
    print("Log directory already exists")

# File names
logFileName = logDir + "/update.log"
natlFileName = dataDir + "/natlData.csv"
nepacFileName = dataDir + "/nepacData.csv"
ibtracsFileName = dataDir + "/ibtracsData.csv"
nameMappingFile = dataDir + "/nameMapping.txt"


logFile = open(logFileName,'w')

""" Define output shapefile names """
if WEBMERC:
    goodSegmentFileName = resultsDir+'/NewSegments_WebMerc'
    goodStormFileName = resultsDir+'/NewTracks_WebMerc'
else:
     goodSegmentFileName = resultsDir+'/NewSegments_WGS84'
     goodStormFileName = resultsDir+'/NewTracks_WGS84'


""" Define JSON filenames """
namesJS = resultsDir + '/stormnames.js'
yearsJSON = resultsDir + '/hurricaneYears.json'

"""--------------------------------------------------------------------"""

""" Specify what HURDAT years to run.  If hFIles is empty, then skip HURDAT
    (ONLY USED FOR TESTING IBTrACS SPECIFIC CODE) """
hFiles = [natlFileName, nepacFileName]
hBasin = ["NA","EP"]
#hFiles = []



""" Define EPSG code for needed projection.
"""
if WEBMERC:
    earthRadius = 6378137.0
    earthCircumference = math.pi * 2.0 * earthRadius
    """ Define EPSG:3857 -- WGS84 Web Mercator (Auxiliary Sphere) Projection string
        http://spatialreference.org/ref/sr-org/7483/ """
#    epsg = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs'
#    epsg = 'PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Mercator"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1]]'
    """ OGC WKT """
#    epsg = 'PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"],AUTHORITY["EPSG","3857"]]'
#==============================================================================
#     epsg='PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",'+
#         'SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],'+
#         'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,'+
#         'AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,'+
#         'AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],'+
#         'PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],'+
#         'PARAMETER["scale_factor",1],PARAMETER["false_easting",0],'+
#         'PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],'+
#         'AXIS["X",EAST],AXIS["Y",NORTH],EXTENSION["PROJ4",'+
#         '"+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"],AUTHORITY["EPSG","3857"]]'
#
#==============================================================================
    """ ESRI prj file below """
    epsg = 'PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0]]'
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
ensoLookup = loadENSODict.ensoDict()
""" Get NHC Storm reports for HURDAT storms from:
             http://www.nhc.noaa.gov/TCR_StormReportsIndex.xml (DLE)
"""
rptLookup = {}
rptLookup = loadStormReportDict.rptDict()
Missing=[None, None]

""" Get Crosswalk table to use to replace HURDAT2 filenames with IBTrACS
    names to construct links for Storm Data Pages frum
    http://ibtracs.unca.edu/index.php?name=...
"""
detailsBaseURL = "http://ibtracs.unca.edu/index.php?name=v04r00-"
ibName = {}
with open(nameMappingFile, 'r') as cwFile:
     while True: # With this and the below break, read to EOF
         lineVals = cwFile.readline()
         if not lineVals: # Finds EOF
             break # Break on EOF
         else: # Data read: Parse it and test to see if it is a new storm
             vals = lineVals.split()
             if "multiple" in lineVals:
                 """ When storms are in multiple basins, use the ATCF IDs,
                 NOTE BENE: there can be more than one! """
                 atcfID = [s for s in vals if "atcf" in s]
                 for thisID in (atcfID):
                     thisKey = thisID[:-6]
                     ibName[thisKey] = vals[0].strip()
             elif "hurdat" in lineVals:
                 ibName[vals[1]] = vals[0].strip()
#                 print(vals, "\n ibName[",vals[1],"] is ", vals[0],"\n",
#                       ibName[vals[1]])#

""" Processing functions """

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

    """ New (9 June 2016) logic: classify everything as a tropical
        system according to its wind speed.  Then only reclassify those
        that specifically are listed as extra-tropical.  This should get
        rid of many of the NR results, which occur in areas beyond the US
        reporting areas and which do not use the Saffir-Simpson Scale.
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
        catSuffix = 'TS' # Storm
    elif wind >= 0:
        catSuffix = 'TD' # Depression
    else:
        catSuffix = "NR"
#        print('No Wind speed, Nature, wind, suffix = ',nature,wind,catSuffix)

    """ Now figure out what it is """
    if (nature[0] == 'E'):
        return 'ET'
    else:
        return catSuffix

#==============================================================================
"""------------------------END OF getCat-------------------------------"""

""" getWindPres function to find none NaN wind and pressure in data """
def getWindPres(values):
    windSpd = ' ' # Default to missing value
    pressure = ' ' # Default to missing value

    # Wind columns in order of number of observations in IBTrACSv04r00
    possibles = [23,    # USA
                 129,   # DS824
                 10,    # WMO
                 57,    # CMA (China)
                 134,   # TD9636
                 144,   # Neumann
                 62,    # HKO (Hong Kong)
                 45,    # TOKYO
                 95,    # BOM (Australia)
                 138,   # TD9635
                 149,   # MLC
                 75,    # REUNION (France)
                 124,   # WELLINGTON (New Zealand)
                 120,   # NADI (Fiji)
                 67]    # NEWDELHI


    for i in possibles:
        if(values[i] != ' '): # Good data exists, use it
            windSpd = values[i]
            pressure = values[i+1]
            break

    return (windSpd, pressure)

"""------------------------END OF getWindPres-------------------------------"""


""" Create needed Objects """
class Storm(object):
    def __init__(self,uid,name):
        self.uid = uid.strip()
        self.name = name.strip()
        self.basin = None
        self.startTime = None
        self.endTime = None
        self.maxW = float(-1.)
        self.minP = float(9999.)
        self.startLat = 0.0
        self.startLon = 0.0
        self.numSegs = 0
        self.maxSaffir = "NR"
        self.enso = "Y"
        self.source = ""  # 0 = IBTrACS, 1 or 2 = HURDAT2 Atl, NEPAC
        self.segs = []

class Observation(object):
    def __init__(self,time,lat,lon,wsp,pres,nature):
        try:
            self.time = dt.datetime.strptime(time,'%Y-%m-%d %H:%M:%S')
#            break
        except ValueError:
            try:
                self.time = dt.datetime.strptime(time,'%m/%d/%Y %H:%M')
            except:
                pass
        self.startLat = float(lat)
        self.startLon = float(lon)
        if wsp == ' ' or float(wsp) < 0 : # N.B. ' ' is the IBTrACSv04 no data value
            self.wsp = float(-1.0)
        else:
            self.wsp = float(wsp)
        if pres == ' ' or float(pres) < 0:
            self.pres = float(-1.0)
        else:
            self.pres = float(pres)
        self.nature = nature.strip()

class Segment(Observation):
    def __init__(self,time,lat,lon,wsp,pres,nature):
        super().__init__(time,lat,lon,wsp,pres,nature)
        self.endLat = float(lat)
        self.endLon = float(lon)
        self.saffir = ""
        self.enso = "X"
        self.amm = "U"
        self.pdo = "U"
        self.amo = "U"


""" Main processing begins here   """


""" Create an empty list to hold allStorms
    and initialize the total storm counter """
allStorms = []
provisionalStorms = []
ibProvisional = 0
#numStorms = -1
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
ibFiles = [ibtracsFileName]
#ibFiles = []
ibNum = 0 # Initialize IBTrACS storm counter,
          # it will increment when storm end is found
ibSkipNum = 0  # Number of NA and EP storms skipped to prevent HURDAT2 duplicates
for i, file in enumerate(ibFiles):
#    print (i, file)
#    print ('IBTrACS file: ', file)
    with open(ibtracsFileName, "r") as rawObsFile:
         head1 = rawObsFile.readline()
         head2 = rawObsFile.readline()
         head3 = rawObsFile.readline()
    #     print(head1, head2, head3)
         """ Read first IBTrACS Record """
         lineVals = rawObsFile.readline() # First Storm record in IBTrACS
         vals = lineVals.split(",")
         """ The vals used has changed with V04r00.  See pdf documentationon
         IBTrACS website for all the possibilites.  We will be using the 'USA'
         values for winds and pressure that should be similar to what was
         previously provided as a 'CSC' version of the IBTrACSv03 data. """
#         print(vals)

         """ Parse vals() to find non-null wind and pressure values from
             appropriate preporting agency """

         tmpWind, tmpPres = getWindPres(vals)


         """ Create first storm """
         thisStorm = Storm(vals[0],          # Unique IBTrACS ID
                           vals[5].strip())  # Name, spaces removed
    #     observation = Segment(vals[6],  # ISO 8601 Time
         observation = Segment(vals[6],  # ISO 8601 Time
                               vals[8], # Lat
                               vals[9], # Lon
                               tmpWind, # Wind from best estimate
                               tmpPres, # Pressure from non-missing
                               vals[7] ) # Nature

         observation.startLon = observation.startLon if observation.startLon <= 180.0 else observation.startLon - 360.
         thisStorm.segs.append(observation)
         thisStorm.startTime = observation.time
         thisStorm.startLon = observation.startLon
         thisStorm.startLat = observation.startLat
         if(LABEL_PROVISIONAL & (vals[13] == 'PROVISIONAL') ):
             thisStorm.name = thisStorm.name + " " \
                 + thisStorm.startTime.strftime('%Y') \
                 + "(P)"
            #  print("Labeling as provisional: ", thisStorm.name )
         else:
             thisStorm.name = thisStorm.name + " " \
             + thisStorm.startTime.strftime('%Y')

         # enter end time in case this is only observation.
         thisStorm.endTime = observation.time
         print(thisStorm.startTime)
         nseg = 1
         thisStorm.source = 0            # Flag data source as IBTrACS
         thisStorm.basin = vals[3].strip()
         """ First storm and observation entered, begin looping """
         while True: # With this and the below break, read to EOF
             lineVals = rawObsFile.readline()
             if not lineVals: # Finds EOF
                 break # Break on EOF
             else: # Data read: Parse it and test to see if it is a new storm
                 vals = lineVals.split(",")
                 if vals[0] == thisStorm.uid :  # Same storm so add the record
                     tmpWind, tmpPres = getWindPres(vals)
                     observation = Segment(vals[6],  # ISO 8601 Time
                                           vals[8], # Lat
                                           vals[9], # Lon
                                           tmpWind, # Wind from best estimate
                                           tmpPres, # Pressure from non-missing
                                           vals[7] ) # Nature
                     observation.startLon = observation.startLon if observation.startLon <= 180.0 else observation.startLon - 360.
                     ibHour = observation.time.hour*100+observation.time.minute
                     if NO391521 and (ibHour == 300 or ibHour == 900 or
                                      ibHour == 1500 or ibHour == 2100):
                        pass #Skip writing this observation
                     else:
                         thisStorm.endTime = observation.time #update end time
                         thisStorm.segs.append(observation)
                         nseg += 1
                 else: #Found a new storm so...
                     thisStorm.numSegs = len(thisStorm.segs)
                     """ Check if we are keeping provisional storms and
                         save storm appropriately """
                     if (OMIT_PROVISIONAL & (vals[13] == 'PROVISIONAL') ):
                         # Add old storm to provisionalStorms
                         ibProvisional += 1
                         print('Provisional storm ', ibProvisional)
                         provisionalStorms.append(thisStorm)
                     else:
                         """ Only keep the storm if there is more than ONE observation: """
                         if(thisStorm.numSegs > 1):
#                             # Skip storms in NA or EP to prevent duplicates with HURDAT2 12/12/2016
#                             if(thisStorm.basin[0:2] != "NA" and thisStorm.basin[0:2] != "EP"):
                             allStorms.append(thisStorm) # Add old storm to allStorms
#        #                         print("IBTrACS basin",thisStorm.basin)
#                             else:
#                                 ibSkipNum += 1
#        #                         print("Duplicate in basin",thisStorm.basin)
                         else:
                             numSinglePoint += 1
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
                     tmpWind, tmpPres = getWindPres(vals)
                     observation = Segment(vals[6],  # ISO 8601 Time
                                           vals[8], # Lat
                                           vals[9], # Lon
                                           tmpWind, # Wind from best estimate
                                           tmpPres, # Pressure from non-missing
                                           vals[7] ) # Nature
                     observation.startLon = observation.startLon if observation.startLon <= 180.0 else observation.startLon - 360.
                     thisStorm.segs.append(observation)
                     thisStorm.startTime = observation.time
                     thisStorm.startLon = observation.startLon
                     thisStorm.startLat = observation.startLat
                     # enter end time in case this is only observation.
                     if(LABEL_PROVISIONAL & (vals[13] == 'PROVISIONAL') ):
                         thisStorm.name = thisStorm.name + " " \
                             + thisStorm.startTime.strftime('%Y') \
                             + "(P)"
                        #  print("Labeling as provisional: ", thisStorm.name )
                     else:
                         thisStorm.name = thisStorm.name + " " \
                         + thisStorm.startTime.strftime('%Y')
                     thisStorm.endTime = observation.time
                     nseg = 1 # New storm ready for next record
                     thisStorm.source = 0 # Flag data source as IBTrACS
                     thisStorm.basin = vals[3].strip()
         """ EOF found on IBTrACS: Write last data and close out """
         thisStorm.numSegs = len(thisStorm.segs)
         """ Only keep the storm if there is more than ONE observation: """
         if (OMIT_PROVISIONAL & (vals[13] == 'PROVISIONAL') ):
             # Add old storm to provisionalStorms
             ibProvisional += 1
             print('Provisional storm ', ibProvisional)
             provisionalStorms.append(thisStorm)
         else:
             if(thisStorm.numSegs > 1):
#                 # Skip storms in NA or EP to prevent duplicates with HURDAT2 12/12/2016
#                 if(thisStorm.basin[0:2] != "NA" and thisStorm.basin[0:2] != "EP"):
                 allStorms.append(thisStorm) # Add old storm to allStorms
#                 else:
#                     ibSkipNum += 1
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

#==============================================================================

#==============================================================================
hstormNum = [0,0]
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
            #numStorms += 1
            hstormNum[i] += 1
            vals = lineVals.split(",")
            #print ("vals = ",vals[0],vals[1],vals[2], len(vals))
            thisStorm = Storm(vals[0],  # Create new storm using Unique ID
                              vals[1].strip())  # and Name w/out spaces

            """ If this storm has an IBTrACS ID, use it instead.
            NOTE BENE: The IBTrACS crosswalk file prepends a "b" on to the
            HURDAT2 (and other) id values.  Therefore, we need to prepend that
            in the test below. """
            testUID = 'b'+thisStorm.uid.lower()
            if (testUID) in ibName:
#                print('Swapping IDs! HURDAT ID, ',thisStorm.uid,
#                      ', IBTrACS ID, ', ibName[testUID])
                thisStorm.uid = ibName[testUID].strip()

            thisStorm.numSegs =  int(vals[2])    # Number of Observations
            thisStorm.source = i + 1 # Flag data source as HURDAT ATL or NEPAC
            thisStorm.basin = hBasin[i]
#            print(thisStorm.uid, thisStorm.name, thisStorm.numSegs)

            for ob in range(thisStorm.numSegs):
                lineVals = rawObsFile.readline()
                if ( (not lineVals) # Finds EOF or any blank line
                or lineVals == "\n" or lineVals == "\r"
                or lineVals == "\n\r"): # lineVals is false at EOF
                    break # Break on EOF
                """ Create a new observation record """
                vals = lineVals.split(",") # Split the record into fields
                """ Format 2 time fields to one ISO format """
                otime = vals[0][0:4] +"-"+vals[0][4:6]+"-"+vals[0][6:] + " "
                otime += vals[1][1:3] + ":" + vals[1][3:] + ":00"

                if (vals[4][len(vals[4])-1] == "N"):
                    lat = float(vals[4][:len(vals[4])-1])
                else:
                    lat = -1. * float(vals[4][:len(vals[4])-1])
                try:
                    if vals[5][len(vals[5])-1] == "E":
                        lon = float(vals[5][:len(vals[5])-1])
                        if(lon > 180.0): # Correct for mis-entered Lon values
                            lon = lon - 360.
                    else:
                        lon = -1. * float(vals[5][:len(vals[5])-1])
                        if(lon < -180.0): # Correct for mis-entered Lon values
                            lon = 360 + lon
                except:
                    print("Bad lon on ob,vals storm",
                          ob,vals,thisStorm.name)
                    exit
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
            thisStorm.startLon = thisStorm.segs[0].startLon
            thisStorm.startLat = thisStorm.segs[0].startLat
            #thisStorm.name = thisStorm.name +" "+ thisStorm.startTime[:4]
            if(LABEL_PROVISIONAL & (vals[13] == 'PROVISIONAL') ):
                thisStorm.name = thisStorm.name + " " \
                     + thisStorm.startTime.strftime('%Y') \
                     + "(P)"
                # print("Labeling as provisional: ", thisStorm.name )
            else:
                thisStorm.name = thisStorm.name + " " \
                 + thisStorm.startTime.strftime('%Y')
            thisStorm.endTime = thisStorm.segs[len(thisStorm.segs)-1].time
            """ Only keep the storm if there is more than ONE observation: """
            if(thisStorm.numSegs != len(thisStorm.segs)):
                print ("Error in Hurdat data record.  Segment count mismatch")
                thisStorm.numSegs = len(thisStorm.segs)
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
    Use storm.source field to pick either HURDAT or IBTrACS storms
    based on value of use_HURDAT boolean 

    With new IBTrACS crosswalk file to replace HURDAT2 storm ids with IBTrACS 
    storm ids, we can sort on those instead of names (which don't work well)
    
    Can now sort on UID and only need to check successive storms for duplicates
    """

allSorted = sorted(allStorms, key = lambda storm: storm.uid)

allStorms = [] # Clear allStorms variable to use for unique storms
allStorms.append(allSorted[0]) # Add first storm to the non-duplicate list

nUnique = 1 # Initialize number of unique storms
nDups = 0   # Initialize number of duplicate storms
dupIndex = nUnique-1 # Index of last added storm.  Only need to check this storm.

for i in range(1,len(allSorted)):  # Cycle through all the Sorted storms
    if(allSorted[i].uid == allStorms[dupIndex].uid and allSorted[i].basin == allStorms[dupIndex].basin):
        # Duplicate so pick according to USE_HURDAT flag
        if USE_HURDAT:
            if allSorted[i].source > 0: #This is a HURDAT record so replace old one
                allStorms[dupIndex] = allSorted[i]
            else: # The existing allStorm record is HURDAT, so keep it
                pass
        else: # Want to use IBTrACS for duplicates
            if allSorted[i].source > 0: #The new record is HURDAT, so skip it
                pass
            else: # The existing allStorm record is HURDAT, so replace it
                allStorms[dupIndex] = allSorted[i]
        nDups = nDups + 1
    else: # not a duplicate, so add it to allStorms and increment nDups
        allStorms.append(allSorted[i])
        dupIndex = nUnique                                         
        nUnique = nUnique + 1
                                                 

""" -------------------- All storms are now unique -------------------- """

""" Now process unique storms for QA/QC and finding Saffir-Simpson value """
# =============================================================================
# """ Make a list of all the Nature types. Needed for setting up Category logic"""
#  allNatures = []
# 
# =============================================================================
""" Make lists for names and years.  Needed for JSON files used by HHT site."""
stormNames = []
stormYears = []
#for i, storm in enumerate(allStorms[11700:11802:4]):
#for i, storm in enumerate(allStorms[1:3]):
for i, storm in enumerate(allStorms):
    """loop through segments, skipping last"""
    storm.numSegs = len(storm.segs)
    jLast = storm.numSegs-1
    j = -1  # Make a new counter in case we add segments by splitting around 180
    for jj in range(0,jLast):
        j+= 1

        """ Find end Lat and Lon for each segment, correcting if needed"""
        """ Make sure LONGITUDE does not change sign across the +-180 line
            Fix this by adjusting the STARTLON of the next segment """
        if abs(storm.segs[j].startLon - storm.segs[j+1].startLon) > 270.:
            """ Lon crosses 180, so """
            if (not BREAK180):
                """ Adjust next startLons so sign stays consistent. This gets
                    all following lons as we iterate through them. """
                adjLon = (
                    math.copysign(360.0,storm.segs[j].startLon)
                    + storm.segs[j+1].startLon)
                print('Adjusting Lon wrap-around: Lon(i), Lon(i+1), adjLon',
                      storm.segs[j].startLon, storm.segs[j+1].startLon,adjLon)
                storm.segs[j+1].startLon = adjLon
        """ put adjusted or NOT adjusted start lat & lon at (j+1)
            in end lat/lon for (j)"""

        """ NOTE BENE: If start and end are too close, offset End slightly """
        segLength = math.sqrt((storm.segs[j+1].startLat-
            storm.segs[j].startLat)**2
            + (storm.segs[j+1].startLon-storm.segs[j].startLon)**2)
        if(segLength <0.14):
            #print('Tweaking identical points, segLength = ', segLength)
            storm.segs[j+1].startLat += 0.001
            storm.segs[j+1].startLon += 0.001

        storm.segs[j].endLat = storm.segs[j+1].startLat
        storm.segs[j].endLon = storm.segs[j+1].startLon
        """ ---------------------END 180 Stuff ----------------------------"""


        """ --- Saffir-Simpson value for each segment"""
        storm.segs[j].saffir = getCat(storm.segs[j].nature,
                                    (storm.segs[j].wsp))
#==============================================================================
#         """ For each segment in the storm find: """
# #        allNatures.append(storm.segs[j].nature)
#         allNatures.append(storm.segs[j].saffir)
#
#==============================================================================
        """ Get data for ENSO stage for each segment by start time """
       # thisKey = storm.segs[j].time[:7]
        thisKey = storm.segs[j].time.strftime('%Y-%m')
        storm.segs[j].enso = ensoLookup.get(thisKey) if ensoLookup.get(thisKey) != None else "U"
        """ Find Max Winds and Saffir-Simpson and Min Pressures """
        if (storm.segs[j].wsp > storm.maxW && storm.segs[j].saffir != "ET"): # New Max found so update MaxW and SS
            storm.maxW = storm.segs[j].wsp
            storm.maxSaffir = storm.segs[j].saffir
        if storm.segs[j].pres < storm.minP and storm.segs[j].pres > 0:
            storm.minP = storm.segs[j].pres

    """ Now need to process the very last segment """
    """ --- ending Lat and Lon for each segment is just the same
    starting location, but offset by 0.01 degrees.
    This allows for the creation of a valid attributed line for every
    actual observation."""
    storm.segs[jLast].endLat = float(storm.segs[jLast].startLat) - 
                                   (float(storm.segs[jLast].startLat) - float(storm.segs[jLast-1].startLat)) * 0.005
   storm.segs[jLast].endLon = float(storm.segs[jLast].startLon) - 
                                   (float(storm.segs[jLast].startLon) - float(storm.segs[jLast-1].startLon)) * 0.005
 #   storm.segs[jLast].endLon = float(storm.segs[jLast].startLon + 0.0001)

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
    storm.segs[jLast].enso = ensoLookup.get(thisKey) if ensoLookup.get(thisKey) != None else "U"
    
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

stormFields = [
               ['STORM_ID','C','56'],
               ['NAME','C','81'],
               ['BEGIN_DATE','D','8'],
               ['END_DATE','D','8'],
               ['MAX_WIND','N','9'],
               ['MIN_PRESS','N','10'],
               ['MAXSSSCALE','C','5'],
               ['BASIN','C','10'],
               ['YEARS','C','10'],
               ['MONTHS','C','10'],
               ['NHC_URL','C','254'],
               ['IBTRACSURL','C','254']
            #    ['DateRange','C','140'],
            #    ['FiltBasins','C','10'],
            #    ['FiltMaxSS','C','10'],
            #    ['In10sOrder','N','10'], # End of Previous Attributes
            #    ['NumObs','C','10'],
            #    ['ENSO','C','10']
               ]
#==============================================================================
# stormFields = ['UID','Name','StartDate','EndDate','MaxWind','MinPress',
#                'NumObs','MaxSaffir','ENSO']
#==============================================================================
""" Create and initalize the fields for the needed Tracks Shapefiles """
#goodTracks = shapefile.Writer(shapefile.POLYLINE) #One line & record per storm
goodTracks = shapefile.Writer(goodStormFileName) #One line & record per storm
goodTracks.autobalance = 1 # make sure all shapes have records
for attribute in stormFields:
    goodTracks.field(attribute[0],attribute[1],attribute[2]) # Add Fields


""" For SEGMENTS : """
segmentFields = [
                 ['SEGMENT_ID','N','10'],
                 ['STORM_ID','C','58'],
                 ['NAME','C','150'],
                 ['TIME','C','20'],
                 ['MAX_WIND','N','9'],
                 ['MIN_PRESS','N','10'],
                 ['SS_SCALE','C','5'],
                 ['BASIN','C','5'],
                 ['BEGIN_LAT','C','10'],
                 ['BEGIN_LON','C','10'],
                 ['END_LAT','C','20'],
                 ['END_LON','C','20'],
                 ['ENSO_STAGE','C','5'],
                 ['AMM_STAGE','C','5'],
                 ['PDO_STAGE','C','5'],
                 ['AMO_STAGE','C','5']
                #  ['DMSW_1min','C','10'],
                #  ['BeginObHr','N','9'],
                #  ['DispDate','C','20'],
                #  ['DMin_Press','C','10'],
                #  ['DDateNTime','C','20'],
                #  ['SegmntOrdr','N','12'],#End of previous attributes
                #  ['Nature','C','20'],
                 ]

""" Create and initalize the fields for the needed Tracks Shapefiles """
goodSegments = shapefile.Writer(goodSegmentFileName) #, shapeType = 3) # New shapefile
goodSegments.autoBalance = 1 # make sure all shapes have records
for attribute in segmentFields: # Add Fields for track shapefile
    goodSegments.field(attribute[0],attribute[1],attribute[2])
"""Lists needed for SCRAMBLING Segments """
goodSegCoords = []
goodSegParams = []
goodSegNum = 0
goodSegIndx = []

stormOID = 0 # Counter to make unique ID number for each storm
segmentOID = 0 # Counter to make unique ID number for each segment

for i, storm in enumerate(allStorms):
    stormOID = stormOID + 1
    basin = storm.basin
    trackCoords = [] # Create list for stormTracks shapefile

    for thisSegment in storm.segs:
        segmentOID = segmentOID + 1

        """ Check for segments spanning the 180 degree line. If they do
            and BREAK180 is true, create multi-part segments. """
        if abs(thisSegment.startLon - thisSegment.endLon) > 270.:
#==============================================================================
#             print('AGAIN Crossing 180')
#==============================================================================
            if BREAK180:
                """ Find new broken coordinates, then convert to webmerc if
                    needed """
                sLon = thisSegment.startLon
                sLat = thisSegment.startLat
                eLon = thisSegment.endLon
                eLat = thisSegment.endLat
                mwLon = math.copysign(180.0,sLon)
                meLon = math.copysign(180.0,eLon)
                """ Interpolate Lat to 180 """
                deltaLon = thisSegment.startLon - (
                (math.copysign(360.0,thisSegment.startLon)
                     + thisSegment.endLon)) # Makes Start & end lons same sign
                mLat = sLat + (sLat - eLat)* ((thisSegment.startLon -
                    math.copysign(180.0,thisSegment.startLon))/
                        deltaLon )

            """ Project to web mercator if need, otherwise just geographic"""
            if WEBMERC:
                sLon = earthRadius * sLon * math.pi/180
                sLat = earthRadius * math.log(math.tan((math.pi/4) + (
                    (sLat*math.pi/180)/2)))
                eLon = earthRadius * eLon * math.pi/180
                eLat = earthRadius * math.log(math.tan((math.pi/4) + (
                    (eLat*math.pi/180)/2)))
                mwLon = earthRadius * mwLon * math.pi/180
                meLon = earthRadius * meLon * math.pi/180
                try:
                    mLat = earthRadius * math.log(math.tan((math.pi/4) + (
                        (mLat*math.pi/180)/2)))
                except ValueError:
                    badLat = mLat
                    print('Bad mLat is ',badLat, 'for i,stormid',i,storm.uid,
                          'from source',storm.source)
                    print('Flush buffer')
                    exit

            """ Done with Web Mercator projection if needed.
                Now put the coordinates into the appropriate lists. """
            """ SEGMENT Coordinates """
            segCoords = [[[sLon, sLat],[mwLon,mLat]],
                         [[meLon,mLat],[eLon, eLat]]]
#==============================================================================
#             """ DEBUG: Print out info for Georges, which is one of many storms
#                 generating shapefiles with bad geometry due to segments too short. """
#             if(storm.name == 'GEORGES 1998'):
#                 print(sLat,sLon, eLat, eLon,'Total Length = ',
#                       math.sqrt( (sLon - eLon)**2 + (sLat-eLat)**2))
#                 print('  Leg 1 Length = ',
#                       math.sqrt( (sLon - meLon)**2 + (sLat-mLat)**2))
#                 print('  Leg 2 Length = ',
#                       math.sqrt( (mwLon - eLon)**2 + (mLat-eLat)**2))
#             """ -------------------  END DEBUG ----------------------"""
#==============================================================================


            """ Add coordinates to the Track shapefile list """
            trackCoords.append([[sLon, sLat],[mwLon,mLat]])
            trackCoords.append([[meLon,mLat],[eLon, eLat]])
        else:
            """ Project to web mercator if need, otherwise just geographic"""
            if WEBMERC:
                sLon = earthRadius * thisSegment.startLon * math.pi/180
                sLat = earthRadius * math.log(
                    math.tan((math.pi/4) + ((thisSegment.startLat*math.pi/180)/2)))
                eLon = earthRadius * thisSegment.endLon * math.pi/180
                try:
                    eLat = earthRadius * math.log(
                        math.tan((math.pi/4) + ((thisSegment.endLat*math.pi/180)/2)))
                except:
                    print(thisStorm.name, sLon, sLat, thisSegment.endLat)
            else:
                sLon = thisSegment.startLon
                sLat = thisSegment.startLat
                eLon = thisSegment.endLon
                eLat = thisSegment.endLat
            segCoords = [[[sLon, sLat],[eLon, eLat]]]
#==============================================================================
#             """ DEBUG: Print out info for Georges, which is one of many storms
#                 generating shapefiles with bad geometry due to segments to short. """
#             if(storm.name == 'GEORGES 1998'):
#                 print(sLat,sLon, eLat, eLon,'Total Length = ',
#                       math.sqrt( (sLon - eLon)**2 + (sLat-eLat)**2))
#             """ -------------------  END DEBUG ----------------------"""
#==============================================================================

            """ Add coordinates to the Track shapefile list """
            trackCoords.append([[sLon, sLat],[eLon, eLat]])

        """ We need to output these attributes:
        ['STORMID','MSW_1min','BeginObHr','BeginLat','BEGINLON',
                 'Min_Press',
                 'Basin','SS_Scale','DateNTime','DMSW_1min',
                 'DispName','DispDate','DMin_Press','DDateNTime', #End of previous attributes
                 'Nature','ENSO',
                 'EndLon','EndLat'] """
        """ Extra values to match old (pre-2015) database structure """
#        basin = rptLookup.setdefault(storm.name,Missing)[1]
        begObsHour = dt.datetime.strftime(thisSegment.time,'%H%M')
        dateTime = dt.datetime.strftime(thisSegment.time,'%m/%d/%Y %H')
        dispDate = dt.datetime.strftime(thisSegment.time,'%b %d, %Y')
        dispDateTime = dt.datetime.strftime(thisSegment.time,'%b %d, %Y %Hz')


        """ Add this segment's data to the appropriate segments shapefile """
        goodSegCoords.append(segCoords)
        goodSegParams.append([segmentOID,     # Storm Object ID,
                       storm.uid,           # Storm ID
                       storm.name,          # Display Storm Name
                       dateTime,            # Date and Time
                       thisSegment.wsp,     # Max. Sustained Wind
                       thisSegment.pres,    # Min Pressure
                       thisSegment.saffir,  # Saffir Simpson Scale
                       basin,               # Basin
                       thisSegment.startLat,# Begin Lat
                       thisSegment.startLon,# Begin Long.
                       thisSegment.endLat,  # End Lat
                       thisSegment.endLon,
                       thisSegment.enso,    # ENSO Flag
                       thisSegment.amm,    # ENSO Flag
                       thisSegment.pdo,   # ENSO Flag
                       thisSegment.amo    # ENSO Flag
                    #    thisSegment.wsp,     # Display Max. Sustained Wind
                    #    thisSegment.nature,  # Nature (not quite SS)
                    #    dispDate,            # Display Date
                    #    thisSegment.pres,    # Display Min Pressure
                    #    dispDateTime,        # Display Date and Time
                    #    goodSegNum,          # Segment Order, a unique ID
                    #     begObsHour,          # Begin Observation Hour Why?
                       ] )  # End Long.
        goodSegIndx.append(goodSegNum)
        goodSegNum += 1


    """ Find ENSO state for start of the storm """
    thisKey = storm.segs[0].time.strftime('%Y-%m')
    storm.enso = ensoLookup.get(thisKey) if ensoLookup.get(thisKey) != None else "U"

    """ Extra values to match old (pre-2015) database structure """
    rptURL = rptLookup.setdefault(storm.name,Missing)[0]
    detailsURL = detailsBaseURL + storm.uid
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
    filtClimReg = "Dummy"
    begObDate = dt.datetime.strftime(storm.startTime,'%Y%m%d')
    endObDate = dt.datetime.strftime(storm.endTime,'%Y%m%d')
    """   --------  End of Extra fields   ------------    """
    """ Append track to appropriate stormTracks list """
    numGoodObs += 1
    goodTracks.line(trackCoords ) # Add the shape
    goodTracks.record(#stormOID,     # Storm Object ID,
                   storm.uid,       # Storm_ID
                   storm.name,      # Display Storm Name
                   begObDate, # Begin Observation Date
                   endObDate,   # End Observation Date
                   storm.maxW,      # Max Sustained WInd, 1 min ave period
                   storm.minP,      # Filter Param: Minimum Pressure
                   storm.maxSaffir, # Display Saffir Simpson
                   basin,           # Basin
                   filtYrs,         # Filter Param. Years
                   filtMons,        # Filter Param. Months
                   rptURL,          # Storm Report URL
                   detailsURL          # Storm Report URL
                #    filtClimReg,     # Filter Param. Climate Regions
                #    storm.maxSaffir, # Filter Param. Saffir Simpson 2 letter
                #    storm.enso,
                #    intensOrder,        # Intensity Order (numeric)
                #    # Extra Attributes below
                #    dateRng,         # Display Date Range
                #    storm.numSegs,   # Number of segments in this Track
                   )      # ENSO Flag

    """ Append the names and the begin and end years to lists so that
        JSON files of the unique names and years can be created for use
        in the HHT web application """
    stormNames.append(storm.name)
    stormYears.append(dt.datetime.strftime(storm.startTime,'%Y') )
    stormYears.append(dt.datetime.strftime(storm.endTime,'%Y') )


""" All done, so """
"""Then scramble Segments if needed.
    Then populate Segments shapefile"""
if (SCRAMBLE):
    random.shuffle(goodSegIndx)
for i in goodSegIndx:
    tmp = goodSegCoords[i]
    goodSegments.line(goodSegCoords[i])
    goodSegments.record(*goodSegParams[i])


""" Save shapefile """

goodSegments.close()
# create the PRJ file
prj1 = open("%s.prj" % goodSegmentFileName, "w")
prj1.write(epsg)
prj1.close()

goodTracks.close()
prj3 = open("%s.prj" % goodStormFileName, "w")
prj3.write(epsg)
prj3.close()

"""Create JSON/js files for unique storm names and unique years."""
uniqueNames = sorted(list(set(stormNames)))
uniqueYears = sorted(list(set(stormYears)))
uniqueYears[:0] = ['All']
fNamesJS = open(namesJS,'w')
nameString = 'var stormnames = ' + json.dumps(uniqueNames)
fNamesJS.write(nameString)
fNamesJS.close()
fYears = open(yearsJSON,'w')
json.dump(uniqueYears,fYears)
fYears.close()
#logFile.close()

print("\n    All IBTrACS: {0}, Skipped NA and EP: {1}, Used: {2}".format(
        ibNum, ibSkipNum, ibNum-ibSkipNum),
        "\n    HURDAT2_ATL: {0}, HURDAT2_NEPAC: {1}".format(
        hstormNum[0], hstormNum[1]),
        "\nQA: TOTAL STORMS INGESTED = {0}\n".format(
        (ibNum-ibSkipNum)+hstormNum[0]+hstormNum[1]),
        "\nQA: Single Obs storms removed: {0}, Multi-Obs storms kept: {1}"
        .format(numSinglePoint,len(allSorted)),
        "\n    STORMS LENGTH CHECKED = {0} \n    (Should equal total ingested.)\n"
        .format(len(allSorted)+numSinglePoint),
        "\nQA: Duplicate storms removed: {0}, Unique storms = {1}"
        .format(nDups,len(allStorms)),
        "\n    STORMS PROCESSED for DUPLICATES = {0}\n".format(
        nDups+len(allStorms)),
        "   (This should equal number of Multi-obs storms.)")
print ("\nQA: NO CHECK FOR MISSING WINDS AND PRESSURE")
print("\n\nQA: If the above QA numbers are consistent, there will be " +
              str(numGoodObs) + ' unique storms in the shapefiles')

logFile.write("\n    All IBTrACS: "+str(ibNum) +
              " Skipped NA and EP: " + str(ibSkipNum) +
              ", Used: " + str(ibNum-ibSkipNum) +
              "\n    HURDAT2_ATL: " + str(hstormNum[0]) +
              " HURDAT2_NEPAC: " + str( hstormNum[1]) +
              "\nQA: TOTAL STORMS INGESTED (sum IBTracs USED and HURDAT2) = " +
              str(ibNum-ibSkipNum+hstormNum[0]+hstormNum[1]))
logFile.write("\n\nQA: Single Obs storms removed: " + str(numSinglePoint) +
              " Multi-Obs storms kept: " + str(len(allSorted)) +
              "\n    SUM OF STORMS LENGTH CHECKED = " + str(len(allSorted)+numSinglePoint) +
              "\n    (Should equal total ingested.)")
logFile.write("\n\nQA: Duplicate storms removed: "+str(nDups) +
              ", Unique storms = " + str(len(allStorms)) +
              "\n    STORMS PROCESSED for DUPLICATES = " +
              str(nDups+len(allStorms)) +
              "\n    (This should equal number of Multi-obs storms.)")
logFile.write("\n\nQA: NO CHECK FOR MISSING WINDS AND PRESSURE")

logFile.write("\n\nQA: If the above QA numbers are consistent, there will be " +
              str(numGoodObs) + ' unique storms in the "good" shapefiles')

logFile.close()
