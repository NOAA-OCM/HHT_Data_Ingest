# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26 14:12:10 2015

@author: Dave.Eslinger
"""
import requests

""" This bit will get the raw text data from the CPO data site """
ensoURL = 'http://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt'
print (ensoURL)
r = requests.get(ensoURL)
lines = r.text.split("\n")
numLines = len(lines)
print("\n",numLines,"lines in initial data file.  First header line and\n",
      "any empty lines at end will be dropped")

vals=[]

for k in range(1,numLines): # start at 1 to skip header row
    foo = lines[k].split()
    if len(foo) > 0:
        isoYRMON = "%s-%02i" % ((foo[0]),int(foo[1]))
        vals.append([isoYRMON,float(foo[4])])
numRows = len(vals)
enso = [0]*numRows # Initialize ENSO state to Neutral flag
print("\n Data record length is ",len(enso),"\n") 
print(vals[0][:3],vals[numRows-3:])
    
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
#    print(k,vals[k],ave3Mon[k], rawENSOState[k])
#==============================================================================
#     else:
#         rawENSOState[k] = (0)
#==============================================================================
""" Now do the last element, averaging it and the second to last element: """
k = numRows-1
ave3Mon[k] = (vals[k-1][1]+vals[k][1])/2
#print(k,"%00.1f" % (ave3Mon[k]))
if ave3Mon[k] <= -0.5:
    rawENSOState[k] = (-1)
elif ave3Mon[k] >= 0.5:
    rawENSOState[k] = (1)
        
#print(k,vals[k],ave3Mon[k], rawENSOState[k])

testStat = [0]*numRows
for l in range(2,numRows-3):
    testStat[l] = sum(rawENSOState[(l-2):(l+3)])
    if testStat[l] == 5:
        enso[l-2:l+3] = [1]*5
    elif testStat[l] == -5:
         enso[l-2:l+3] = [-1]*5
#    print(l,vals[l],rawENSOState[l],testStat,"from:",
#          rawENSOState[(l-2):(l+3)],enso[l])

for k in range(0,50): 
   print("Finals:",k,vals[k],"%0.1f"%(ave3Mon[k]),rawENSOState[k],testStat[k],enso[k])
for k in range(700,numRows): 
   print("Finals:",k,vals[k],"%0.1f"%(ave3Mon[k]),rawENSOState[k],testStat[k],enso[k])


