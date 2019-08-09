# -*- coding: utf-8 -*-
"""
HHT_AnnualUpdate.py

A script for reading and reformatting 3 data files for HHT data reprocessing.
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
         2015-07-20: Renamed for operational use.  Now also creates two needed
             index files: hurricaneYears.json and stormnames.js. These are
             used by the Historical Hurricane Tracks site
             (http://coast.noaa.gov/hurricanes) for searching by year and/or
             by name.
         2016-04-20: Matt Pendleton editing for test runs for 2015 data
         2016-05-16: test runs with 2015 ibtracs and fixed Hurdat2
         2017-06-22: Shapefile attribute name changes for new database
         2018-06-11: Updated for 2017 data.  Should be the last version of V03.
         2019-07-15: Updating for 2018 storms, need to read in IBTrACS V04.
                     Change columns for IBTrACSV0400, clean up old comments DLE
         2019-07-24: Fixed for pyshp 2.1.0, which had very different calls, etc.
                     than previous version used.  This version appears to be
                     supported by Anaconda. DLE

"""

import os
import math
import random
import json
import datetime as dt

""" If shapefile package is not available, add it using conda.
    Try this first to see if the Anaconda folks have added it to their library:
        conda install pyshp
    If that doesn't work, get it from another archive, such as conda-forge:
        conda install --channel https://conda.anaconda.org/conda-forge pyshp
    """
import shapefile

"""  These next two packages are custom packages for this progam.  They
    should be in the same directory as HHT_Annualupdate
    """
import ensoDownload # Local python module
import stormReportDownload # Local python module

""" Declarations and Parameters """
SCRAMBLE = True
WEBMERC = True
BREAK180 = True
OMIT_PROVISIONAL = False
LABEL_PROVISIONAL = True
TESTING = False

""" If NO391521 is True, then omit obs at 03:00, 09:00, 15:00 and 21:00 from IBTrACS.
    These appear to be poor quality (DLE's observation) records from different
    reporting groups and give the dashed black-colored zig zag look to many
    tracks in the Indian Ocean. """
NO391521 = True

""" Choose to use either HURDAT2 data as the 'base' data layer (a new
    behaviour) or to use IBTrACS as the 'base' depending on the
    use_HURDAT variable: """
use_HURDAT = True
dupRange = 5

"""---------- DEFINE WORKING DIRECTORIES AND FILE NAMES --------------------"""
workDir = "K:/GIS/Hurricanes/HHT/2018_Season/" # C: at work, K: at home
dataDir = workDir + "Data/"  # Data location
if TESTING:
    h2AtlRaw = dataDir + "hurdat2-1851-2018-051019.txt"     # HURDAT2 North Atlantic Data
    h2nepacRaw = dataDir + "hurdat2-nepac-1949-2018-071519.txt" # HURDAT2 NE North Pacific Data
    ibRaw = dataDir + "ibtracs.ALL.list.v04r00.csv" # 2018 storm data
#    ibRaw = dataDir + "ibTESTv04r00.csv"
    # Location & prefix w/out trailing '/' for test data
    resultsDir = workDir + "Results/Test/WithHURDAT"  
else:
    h2AtlRaw = dataDir + "hurdat2-1851-2018-051019.txt"     # HURDAT2 North Atlantic Data
    h2nepacRaw = dataDir + "hurdat2-nepac-1949-2018-071519.txt" # HURDAT2 NE North Pacific Data
    ibRaw = dataDir + "ibtracs.ALL.list.v04r00.csv" # 2018 storm data
#    ibRaw = ""
    resultsDir = workDir + "Results/WithProvisional/"  #  Location for final results


""" Create the needed Results directory if it doesn't exist """
os.makedirs(os.path.dirname(resultsDir),exist_ok=True)
""" Adding an ingest.log ascii file to record QA/QC results in the Results
    directory. """
logFileName = resultsDir + "ingest.log"
logFile = open(logFileName,'w')

""" Specify what HURDAT years to run.  If hFIles is empty, then skip HURDAT
    (ONLY USED FOR TESTING IBTrACS SPECIFIC CODE) """
hFiles = [h2AtlRaw, h2nepacRaw]
hBasin = ["NA","EP"]
#hFiles = []

""" Define output shapefile names """
if WEBMERC:
    goodSegmentFileName = resultsDir+'goodSegments_WebMerc_2018'
    goodStormFileName = resultsDir+'goodTracks_WebMerc_2018'
    missingSegmentFileName = resultsDir+'missingSegments_WebMerc_2018'
    missingStormFileName = resultsDir+'missingTracks_WebMerc_2018'
else:
     goodSegmentFileName = resultsDir+'goodSegments_2018'
     goodStormFileName = resultsDir+'goodTracks_2018'
     missingSegmentFileName = resultsDir+'missingSegments_2018'
     missingStormFileName = resultsDir+'missingTracks_2018'


""" Define JSON filenames """
namesJS = resultsDir + 'stormnames.js'
yearsJSON = resultsDir + 'hurricaneYears.json'

"""--------------------------------------------------------------------"""


""" Define EPSG code for needed projection.  Either Manually or using
    modification of this:

    def getPRJwkt(epsg):
        import urllib
        f=urllib.urlopen("http://spatialreference.org/ref/epsg/{0}/prettywkt/".format(epsg))
        return (f.read())
    Which is from comments in: https://code.google.com/p/pyshp/wiki/PyShpDocs
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
ensoLookup = ensoDownload.ensoDict(dataDir)
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
#"""--------------------------------------------------------------------"""
#def getStormReport(name,year):
#    return "no report"
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
        self.enso = ""
        self.source = ""  # 0 = IBTrACS, 1 or 2 = HURDAT2 Atl, NEPAC
        self.segs = []

class Observation(object):
    def __init__(self,time,lat,lon,wsp,pres,nature):
#==============================================================================
#         self.time = time.strip()
#         print(self.time)
#==============================================================================
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
        if wsp == ' ':        # N.B. This is the IBTrACSv04 no data value
            self.wsp = float(-1.0)
#            self.wsp = float('NaN') #try NaN for missing wind speeds:
#            NAN not working with graphing portion of web site.  Go back to -1 as flag
        else:
            self.wsp = float(wsp)
        if pres == ' ':
            self.pres = float(-1.0)
        else:
            self.pres = float(pres)
        self.nature = nature.strip()

class Segment(Observation):
    def __init__(self,time,lat,lon,wsp,pres,nature):
        super().__init__(time,lat,lon,wsp,pres,nature)
        self.endLat = float(lat)# Test change flot 0 to float9lat/lon) to make non-int.
        self.endLon = float(lon)
        self.saffir = ""
        self.enso = None

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
ibFiles = [ibRaw]
#ibFiles = []
ibNum = 0 # Initialize IBTrACS storm counter,
          # it will increment when storm end is found
ibSkipNum = 0  # Number of NA and EP storms skipped to prevent HURDAT2 duplicates
for i, file in enumerate(ibFiles):
    print (i, file)
    print ('IBTrACS file: ', file)
    with open(ibRaw, "r") as rawObsFile:
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
         print(vals)

         """ Create first storm """
         thisStorm = Storm(vals[0],          # Unique IBTrACS ID
                           vals[5].strip())  # Name, spaces removed
    #     observation = Segment(vals[6],  # ISO 8601 Time
         observation = Segment(vals[6],  # ISO 8601 Time
                               vals[8], # Lat
                               vals[8], # Lon
                               vals[23], # USA_Wind speed Was [10]
                               vals[24], # USA_Pressure
                               vals[7] ) # Nature
         thisStorm.segs.append(observation)
         thisStorm.startTime = observation.time
         thisStorm.startLon = observation.startLon
         thisStorm.startLat = observation.startLat
         if(LABEL_PROVISIONAL & (vals[13] == 'PROVISIONAL') ):
             thisStorm.name = thisStorm.name + " " \
                 + thisStorm.startTime.strftime('%Y') \
                 + "PROVISIONAL"
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
                     observation = Segment(vals[6],  # ISO 8601 Time
                                           vals[8], # Lat
                                           vals[9], # Lon
                                           vals[23], # USA_Wind speed Was [10]
                                           vals[24], # USA_Pressure
                                           vals[7] ) # Nature
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
                             # Skip storms in NA or EP to prevent duplicates with HURDAT2 12/12/2016
                             if(thisStorm.basin[0:2] != "NA" and thisStorm.basin[0:2] != "EP"):
                                 allStorms.append(thisStorm) # Add old storm to allStorms
        #                         print("IBTrACS basin",thisStorm.basin)
                             else:
                                 ibSkipNum += 1
        #                         print("Duplicate in basin",thisStorm.basin)
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
                     observation = Segment(vals[6],  # ISO 8601 Time
                                           vals[8], # Lat
                                           vals[9], # Lon
                                           vals[23], # USA_Wind speed Was [10]
                                           vals[24], # USA_Pressure
                                           vals[7] ) # Nature
                     thisStorm.segs.append(observation)
                     thisStorm.startTime = observation.time
                     thisStorm.startLon = observation.startLon
                     thisStorm.startLat = observation.startLat
                     # enter end time in case this is only observation.
                     if(LABEL_PROVISIONAL & (vals[13] == 'PROVISIONAL') ):
                         thisStorm.name = thisStorm.name + " " \
                             + thisStorm.startTime.strftime('%Y') \
                             + "PROVISIONAL"
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
                 # Skip storms in NA or EP to prevent duplicates with HURDAT2 12/12/2016
                 if(thisStorm.basin[0:2] != "NA" and thisStorm.basin[0:2] != "EP"):
                     allStorms.append(thisStorm) # Add old storm to allStorms
                 else:
                     ibSkipNum += 1
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
                     + "PROVISIONAL"
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
    Use sotrm.source field to pick either HURDAT or IBTrACS storms
    based on value of use_HURDAT boolean """

allSorted = sorted(allStorms, key = lambda storm: storm.startTime)

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
        """ Check data sources.  If they are not IBTrACS vs HURDAT, then
            the storms are not duplicates.
            N.B.: This assumes no intra-dataset duplicates, which seems true.
            **** ALERT *** The above assumption is incorrect.  IBTrACS has
            many self-duplicates.  We now check with those and remove duplicates
            unless they are in different basins. 5/31/2016 DLE """
        if(allSorted[i].source == allStorms[j].source):
            continue # These are from different basins so are not duplicates

        """ There can be duplicates in IBTrACS (and also Hurdat), so need to
        check all storms no matter the source.  However, storms from different
        basins should not be duplicates, so check for that, since those can
        meet the other duplicate requirements, e.g., LIN and CAROLINE."""

        """ Now I'm not so sure about the duplicates w/in data sets.
            Many identified are not duplicates, they start too far apart.
            DLE 12/21/2016   """
        if(allSorted[i].basin != allStorms[j].basin):
            continue # These are from different basins so are not duplicates

        """ Check names, but omit the year from the search string.
            We omit the year to be able to search for the names as substrings
            within each other.  The name is just appended on and will confuse
            the search logic. """
        AsortedName = allSorted[i].name[:len(allSorted[i].name)-5]
        AallName =  allStorms[j].name[:len(allStorms[j].name)-5]
#        AXsortedName = allSorted[i].name[:len(allSorted[i].name)-0]
#        AXallName =  allStorms[j].name[:len(allStorms[j].name)-0]
        AallInSorted = AsortedName.find(AallName)
        AsortedInAll =  AallName.find(AsortedName)
        if AallInSorted + AsortedInAll != -2: # Duplicate names found
            """ First check that BASINS are the same.  Unfortunately,
            there are duplicate storm names in the same year, but in different
            BASINS, esp. for the North Atlantic and Western Pacific in the
            late 1970s"""
            if (allSorted[i].basin != allStorms[j].basin):
                # The basins are different, so not duplicates. CONTINUE to next j loop
                continue
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
        logFile.write('Duplicate set ' + str(nDups) + ' found! \n' +
               'Source\tUID\t\tName\t\tStart Date\t\tBasin\tLon\tLat\n' +
               str(allSorted[i].source) +"\t"+ str(allSorted[i].uid) +"\t"+
               allSorted[i].name +"\t"+
               str(allSorted[i].startTime) +"\t"+
               str(allSorted[i].basin) +"\t"+
               str(allSorted[i].startLon) +"\t"+
               str(allSorted[i].startLat) + ' and \n' +
               str(allStorms[dupIndex].source) +"\t"+
               str(allStorms[dupIndex].uid) +"\t"+
               allStorms[dupIndex].name +"\t"+
               str(allStorms[dupIndex].startTime) +"\t"+
               str(allStorms[dupIndex].basin) +"\t"+
               str(allStorms[dupIndex].startLon) +"\t"+
               str(allStorms[dupIndex].startLat) + "\n")
        if use_HURDAT:
             if allSorted[i].source > 0: #This is a HURDAT record so replace old one
                """ NOTE BENE: If choosing HURDAT over IBTrACS, then replace
                the Unique idenitifier with the IBTrACS ID.  That is needed
                for the Storm Details lookup from the IBTrACS web site!
                DLE 6/13/2016 """
                allSorted[i].uid = allStorms[dupIndex].uid #replace w/ IBTrACS UID
                allStorms[dupIndex] = allSorted[i]
#                allStorms[dupIndex].uid = IBTrACS_uid #And replace HURDAT one with it
             else: # The existing allStorm record is HURDAT, so keep it
                 pass
        else: # Want to use IBTrACS for duplicates
            if allSorted[i].source > 0: #The new record is HURDAT, so skip it
                pass
            else: # The existing allStorm record is HURDAT, so replace it
                # Check for duplicates w/in IBTrACS and use dup w/ longer name
                if allSorted[i].source == allSorted[dupIndex].source:
                    """Pick the storm with the most names assigned to it.
                       This is so that it will show up in searches with any
                       of its potential names.  Also, these tend to be storms
                       with the longer tracks since they are reported by a
                       larger number of reporting groups. """
                    """ STILL NEEDS to Be FIXED """
                    pass
                else:
                    allStorms[dupIndex] = allSorted[i]

    else: # not a duplicate, so copy it to allStorms
        allStorms.append(allSorted[i])

""" -------------------- All storms are now unique -------------------- """

""" Now process unique storms for QA/QC and finding Saffir-Simpson value """
#==============================================================================
# """ Make a list of all the Nature types. Needed for setting up enso logic"""
# allNatures = []
# """ Make lists for names and years.  Needed for JSON files used by HHT site."""
#==============================================================================
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
    storm.segs[jLast].endLat = float(storm.segs[jLast].startLat + 0.0001)
    storm.segs[jLast].endLon = float(storm.segs[jLast].startLon + 0.0001)

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

stormFields = [['STRMTRKOID','N','10'],
               ['STORMID','C','56'],
               ['MaxWindSpd','N','19'],
               ['Basin','C','10'],
               ['Disp_Name','C','81'],
               ['DateRange','C','140'],
               ['Begin_Date','D','8'],
               ['End_Date','D','8'],
               ['SS_Scale','C','10'],
               ['FiltYears','C','10'],
               ['FiltMonths','C','10'],
               ['FiltBasins','C','10'],
               ['FiltMaxSS','C','10'],
               ['Min_Press','N','10'],
               ['StrmRptURL','C','254'],
               ['In10sOrder','N','10'], # End of Previous Attributes
               ['NumObs','C','10'],
               ['ENSO','C','10']]
#==============================================================================
# stormFields = ['UID','Name','StartDate','EndDate','MaxWind','MinPress',
#                'NumObs','MaxSaffir','ENSO']
#==============================================================================
""" Create and initalize the fields for the needed Tracks Shapefiles """
#goodTracks = shapefile.Writer(shapefile.POLYLINE) #One line & record per storm
goodTracks = shapefile.Writer(goodStormFileName) #, shapeType = 3) #One line & record per storm
goodTracks.autobalance = 1 # make sure all shapes have records
missingTracks = shapefile.Writer(missingStormFileName) #, shapeType = 3) #One line & record per storm
missingTracks.autobalance = 1 # make sure all shapes have records
for attribute in stormFields:
    goodTracks.field(attribute[0],attribute[1],attribute[2]) # Add Fields
    missingTracks.field(attribute[0],attribute[1],attribute[2]) # Add Fields


""" For SEGMENTS : """
segmentFields = [['SEGMNTOID','N','10'],
                 ['STORMID','C','58'],
                 ['MaxWindSpd','N','9'],
                 ['BeginObHr','C','9'],
                 ['BeginLat','C','10'],  # Why C?  Is that character? Need a float!
                 ['BeginLon','C','10'],
                 ['Min_Press','C','10'],
                 ['Basin','C','10'],
                 ['SS_Scale','C','10'],
                 ['DateNTime','C','20'],
                 ['DMSW_1min','C','10'],
                 ['DispName','C','150'],
                 ['DispDate','C','20'],
                 ['DMin_Press','C','10'],
                 ['DDateNTime','C','20'],
                 ['Segment_ID','C','12'],#End of previous attributes
                 ['Nature','C','20'],
                 ['ENSO','C','20'],
                 ['EndLat','C','20'],
                 ['EndLon','C','20']]

""" Create and initalize the fields for the needed Tracks Shapefiles """
goodSegments = shapefile.Writer(goodSegmentFileName) #, shapeType = 3) # New shapefile
goodSegments.autoBalance = 1 # make sure all shapes have records
missingSegments = shapefile.Writer(missingSegmentFileName) #, shapeType = 3) # New shapefile
missingSegments.autoBalance = 1 # make sure all shapes have records
for attribute in segmentFields: # Add Fields for track shapefile
    goodSegments.field(attribute[0],attribute[1],attribute[2])
    missingSegments.field(attribute[0],attribute[1],attribute[2])
"""Lists needed for SCRAMBLING Segments """
goodSegCoords = []
goodSegParams = []
goodSegNum = 0
goodSegIndx = []

missingSegCoords = []
missingSegParams = []
missingSegNum = 0
missingSegIndx= []
stormOID = 0 # Counter to make unique ID number for each storm
segmentOID = 0 # Counter to make unique ID number for each segment

for i, storm in enumerate(allStorms):
    stormOID = stormOID + 1
    basin = storm.basin
    trackCoords = [] # Create list for stormTracks shapefile
    """ Find out if this hurricane has any valid pressure or wind speed obs """
    if (float(storm.minP) < 0.0 and float(storm.maxW) < 0.0 ):
        goodStorm = False
    else:
        goodStorm = True
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
        if goodStorm:
            goodSegCoords.append(segCoords)
            goodSegParams.append([segmentOID,     # Storm Object ID,
                           storm.uid,           # Storm ID
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
                           goodSegNum,          # Segment Order, a unique ID
                           # End of Previous Attributes
                           thisSegment.nature,  # Nature (not quite SS)
                           thisSegment.enso,    # ENSO Flag
                           thisSegment.endLat,  # End Lat
                           thisSegment.endLon] )  # End Long.
            goodSegIndx.append(goodSegNum)
            goodSegNum += 1

        else:
            missingSegCoords.append(segCoords)
            missingSegParams.append([segmentOID,     # Storm Object ID,
                           storm.uid,           # Storm ID
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
                         3000000+missingSegNum, # Segment Order, a unique ID
                           # End of Previous Attributes
                           thisSegment.nature,  # Nature (not quite SS)
                           thisSegment.enso,    # ENSO Flag
                           thisSegment.endLat,  # End Lat
                           thisSegment.endLon] )  # End Long.
            missingSegIndx.append(missingSegNum)
            missingSegNum += 1

#==============================================================================
#     print(trackCoords)
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
#    basin = rptLookup.setdefault(storm.name,Missing)[1]
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
    filtClimReg = "Dummy"
    begObDate = dt.datetime.strftime(storm.startTime,'%Y%m%d')
    endObDate = dt.datetime.strftime(storm.endTime,'%Y%m%d')
    """   --------  End of Extra fields   ------------    """
    """ Append track to appropriate stormTracks list """
    if goodStorm:
        numGoodObs += 1
        goodTracks.line(trackCoords ) # Add the shape
        goodTracks.record(stormOID,     # Storm Object ID,
                       storm.uid,       # StormID
                       storm.maxW,      # Max Sustained WInd, 1 min ave period
                       basin,           # Basin
                       storm.name,      # Display Storm Name
                       dateRng,         # Display Date Range
                       begObDate, # Begin Observation Date
                       endObDate,   # End Observation Date
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
        missingTracks.line(trackCoords ) # Add the shape
        missingTracks.record(stormOID,     # Storm Object ID,
                       storm.uid,       # StormID
                       storm.maxW,      # Max Sustained WInd, 1 min ave period
                       basin,           # Basin
                       storm.name,      # Display Storm Name
                       dateRng,         # Display Date Range
                       begObDate, # Begin Observation Date
                       endObDate,   # End Observation Date
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
    random.shuffle(missingSegIndx)
for i in goodSegIndx:
    tmp = goodSegCoords[i]
    goodSegments.line(goodSegCoords[i])
    goodSegments.record(*goodSegParams[i])

for i in missingSegIndx:
    tmp = missingSegCoords[i]
    missingSegments.line(missingSegCoords[i])
    missingSegments.record(*missingSegParams[i])


""" Save shapefile """

goodSegments.close()
# create the PRJ file
prj1 = open("%s.prj" % goodSegmentFileName, "w")
prj1.write(epsg)
prj1.close()

missingSegments.close()
prj2 = open("%s.prj" % missingSegmentFileName, "w")
prj2.write(epsg)
prj2.close()

goodTracks.close()
prj3 = open("%s.prj" % goodStormFileName, "w")
prj3.write(epsg)
prj3.close()

missingTracks.close()
# create the PRJ file
prj4 = open("%s.prj" % missingStormFileName, "w")
prj4.write(epsg)
prj4.close()

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
        "    Single Obs storms removed: {0}, Multi-Obs storms kept: {1}"
        .format(numSinglePoint,len(allSorted)),
        "\nQA: STORMS LENGTH CHECKED = {0} \n(Should equal total ingested.)\n"
        .format(len(allSorted)+numSinglePoint),
        "    Duplicate storms removed: {0}, Unique storms = {1}"
        .format(nDups,len(allStorms)),
        "\nQA: STORMS PROCESSED for DUPLICATES = {0}\n".format(
        nDups+len(allStorms)),
        "    (This should equal number of Multi-obs storms.)")
print ("    Storms with no wind or pressure:",numAllMissing,
       ", Good storm tracks: ",numGoodObs,
       "\nQA: STORMS CHECKED FOR WIND AND PRESSURE VALUES = {0}\n"
       .format(numAllMissing+numGoodObs),
        "    (This should equal number of UNIQUE storms.)\n")
print("\n\nIf the above QA numbers are consistent, there will be ",
      numGoodObs,'\nstorms in the "good" shapefiles,\n',
      "and",numAllMissing,'storms in the "missing" shapefiles. \n',
      'All should be ingested into the database for use on the HHT site.')

logFile.write("\n    All IBTrACS: "+str(ibNum) +
              " Skipped NA and EP: " + str(ibSkipNum) +
              ", Used: " + str(ibNum-ibSkipNum) +
              "\n    HURDAT2_ATL: " + str(hstormNum[0]) +
              " HURDAT2_NEPAC: " + str( hstormNum[1]) +
              "\nQA: TOTAL STORMS INGESTED (sum IBTracs USED and HURDAT2) = " +
              str(ibNum-ibSkipNum+hstormNum[0]+hstormNum[1]))
logFile.write("\n    Single Obs storms removed: " + str(numSinglePoint) +
              " Multi-Obs storms kept: " + str(len(allSorted)) +
              "\nQA: SUM OF STORMS LENGTH CHECKED = " + str(len(allSorted)+numSinglePoint) +
              " (Should equal total ingested.)")
logFile.write("\n    Duplicate storms removed: "+str(nDups) +
              ", Unique storms = " + str(len(allStorms)) +
              "\nQA: STORMS PROCESSED for DUPLICATES = " +
              str(nDups+len(allStorms)) +
              "  (This should equal number of Multi-obs storms.)")
logFile.write("\n    Storms with no wind or pressure: " + str(numAllMissing) +
              ", Good storm tracks: " + str(numGoodObs) +
              "\nQA: TOTAL STORMS CHECKED FOR WIND AND PRESSURE VALUES = " +
              str(numAllMissing+numGoodObs) +
              ", (This should equal number of UNIQUE storms.)\n")
logFile.write("\nIf the above QA numbers are consistent, there will be " +
              str(numGoodObs) + ' storms in the "good" shapefiles,\n' +
      "and "+ str(numAllMissing) + ' storms in the "missing" shapefiles. \n' +
      'All should be ingested into the database for use on the HHT site.')

logFile.close()
