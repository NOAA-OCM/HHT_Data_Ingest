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
print(numLines)
vals = []
rawENSOState = []
rawENSO = []
for k in range(1,numLines): 
    foo = lines[k].split()
    if len(foo) > 0:
        vals.append(foo)
#        print(k,vals[k-1])
        rawENSO.append(float(vals[k-1][4] ))
        if rawENSO[k-1] <= -0.5:
            rawENSOState.append(-1)
        elif rawENSO[k-1] >= 0.5:
            rawENSOState.append(1)
        else:
            rawENSOState.append(0)

 #   print(k,vals[k],rawENSO, rawENSOState[k])

numRows = len(vals)
enso = [0]*numRows # Initialize ENSO state to Neutral flag
print("enso is ",len(enso),"members long")
#==============================================================================
# for k in range(numRows):
#     enso.append(0)
#==============================================================================
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
   print("Finals:",k,vals[k],testStat[k],rawENSOState[k],enso[k])
for k in range(700,numRows): 
   print("Finals:",k,vals[k],testStat[k],rawENSOState[k],enso[k])


