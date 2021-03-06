# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26-30 2015

@author: Dave.Eslinger
         dave.eslinger@noaa.gov
         
The goal of this program was to work out how toaccess the raw data for 
Climate Prediction Center Oceanic Nino Index (ONI) directly from their
online data location. Then we can process the anomolies to produce
ONI values.  

This process will be used in the HHT preprocessing program
to create a dictionary to tag each segment with it's ENSO state.

"""
import configparser
""" Declarations and Parameters from Configuration file"""
config = configparser.ConfigParser()
config.read('./config.ini')
dataDir = config.get('DIRECTORIES','DATA')
ensoFile = dataDir + "/ensoData.txt"

def ensoDict():
# =============================================================================
#     """ This bit will get the raw text data from the CPO data site """
#     ensoURL = config.get('DOWNLOAD','ENSOURL')
# #    ensoURL = 'http://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt'
#     print ("Checking ENSO ONI data from:\n",
#            ensoURL, "\n")
#            
# =============================================================================
    try:
        with open(ensoFile) as r:
            allLines = str(r.read())
            lines = allLines.split("\n") #Web version requires the escape char to be escaped
    except:
        print ("Error reading ENSO FIle")

    numLines = len(lines)
    print("\n",numLines,"lines in initial ENSO data file.  First header line\n",
          " and any empty lines at the end will be dropped")
          
    vals=[]
    
    for k in range(1,numLines): # start at 1 to skip header row
        foo = lines[k].split()
        if len(foo) > 1: # There can be a single quote in the last line
            isoYRMON = "%s-%02i" % ((foo[0]),int(foo[1]))
            vals.append([isoYRMON,float(foo[4])])
    numRows = len(vals)
    enso = ["0"]*numRows # Initialize ENSO state to Neutral flag
    # enso = [0]*numRows # Initialize ENSO state to Neutral flag
    #print("\n Data record length is ",len(enso),"\n") 
    #print(vals[0:3],vals[numRows-3:])
        
    """Now that all data are read in, calculate 3 month 
    running averages """
    rawENSOState = [0]*numRows
    ave3Mon = [0]*numRows
    
    """ Do the first element, averaging it and the second element: """
    k = 0
    ave3Mon[k] = (vals[k][1]+vals[k+1][1])/2
    #print(k,"%00.1f" % (ave3Mon[k]))
    if ave3Mon[k] <= -0.5:
        rawENSOState[k] = (-1)
    elif ave3Mon[k] >= 0.5:
        rawENSOState[k] = (1)
    #print(k,vals[k],ave3Mon[k], rawENSOState[k])
            
    """ Do all the "internal" records """
    for k in range(1,numRows-1): #skip first and last fields
        ave3Mon[k] = round(((vals[k-1][1]+vals[k][1]+vals[k+1][1])/3),1)
    #    print(k,"%00.1f" % (ave3Mon[k]))
        if ave3Mon[k] <= -0.5:
            rawENSOState[k] = (-1)
        elif ave3Mon[k] >= 0.5:
            rawENSOState[k] = (1)
    
    """ Now do the last element, averaging it and the second to last element: """
    k = numRows-1
    ave3Mon[k] = (vals[k-1][1]+vals[k][1])/2
    #print(k,"%00.1f" % (ave3Mon[k]))
    if ave3Mon[k] <= -0.5:
        rawENSOState[k] = (-1)
    elif ave3Mon[k] >= 0.5:
        rawENSOState[k] = (1)
            
    """  Calculate the actual ENSO flag: 
            1 = El Nino, 
            0 = Neutral, 
           -1 = La Nina 
           
         The logic is that if the centered, running sum for any month 
         is +5 or -5, then all 5 of those summed months were in a non-neutral
         ENSO state. They are flagged appropriately.  Unflagged vlaues retain
         their initialized Neutral (0) flag. """
    testStat = [0]*numRows
    for l in range(2,numRows-3):
        testStat[l] = sum(rawENSOState[(l-2):(l+3)])
        if testStat[l] == 5:
            enso[l-2:l+3] = ["P"]*5
            # enso[l-2:l+3] = [1]*5
        elif testStat[l] == -5:
             enso[l-2:l+3] = ["N"]*5
            #  enso[l-2:l+3] = [-1]*5
    #    print(l,vals[l],rawENSOState[l],testStat,"from:",
    #          rawENSOState[(l-2):(l+3)],enso[l])
             
#==============================================================================
#     """ Print the first and last records for comparison w/ CPO web site """
#     for k in range(0,50): 
#        print("Finals:",k,vals[k],"%0.1f"%(ave3Mon[k]),
#              rawENSOState[k],testStat[k],enso[k])
#     for k in range(700,numRows): 
#        print("Finals:",k,vals[k],"%0.1f"%(ave3Mon[k]),
#              rawENSOState[k],testStat[k],enso[k])
#==============================================================================

    """ Now put the needed data into a dictionary and return that """
    ensoState = {}
    for k in range(numRows):
        ensoState[vals[k][0]] = enso[k]
    
    return ensoState
# =============================================================================
# """ This is the test code to simply run the rptDict function by itself 
#     It is not called when the program is included in another program. """        
# foo = ensoDict()
# print (foo)
# 
# =============================================================================
