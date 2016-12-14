# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 11:20:23 2015

@author: Dave.Eslinger

This program will hread an XML listing of Tropical Cyclone Storm Reports from
the National Hurricane Center's list located at:
             http://www.nhc.noaa.gov/TCR_StormReportsIndex.xml
and returns a dictionary of key = nameYear, i.e., "Alberto 1994" and 
 entries of rptURL, e.g., "http://www.nhc.noaa.gov/blahblah/", and
            basin, e.g., "Atlantic"
            
Note that much of this XML code is from the Python documentation at:
https://docs.python.org/3.4/library/xml.etree.elementtree.html#module-xml.etree.ElementTree
    
"""

import urllib.request
import xml.etree.ElementTree as ET

def rptDict():
    rptURL = "http://www.nhc.noaa.gov/TCR_StormReportsIndex.xml"
    file = urllib.request.urlopen(rptURL)
    data = file.read()
    file.close()
    
    root = ET.fromstring(data)
    #print (root.attrib)
    strName = []
    url = []
    syear = []
    basin = []
    for i,storm in enumerate(root):
#        longName.append(storm[0].text)
        longName = storm[0].text.replace("Hurricane ","")
        longName = longName.replace("Tropical Storm ","")
        longName = longName.replace("Tropical Depression ","")
        longName = longName.replace("subtropical Storm ","")
        longName = longName.replace(" (Atlantic)","")
        longName = longName.replace(" (Pacific)","")
        url.append(storm[1].text)
        syear.append(storm[2].text)
#        basin.append(storm[3].text)
        if (storm[3].text) == "Atlantic" :
            basin.append("NA")  # Only for the North Atlantic via HURDAT2
        else:
            basin.append("EP")  # Or for the North East Pacific via HURDAT2  
        #print ((longName + " " + syear[i]))
        strName.append(longName.upper() + " " + syear[i])
        
    """ Now put needed items into a dictionary and return it: """
    reportDict = {}
    for k in range(len(strName)):
        reportDict[strName[k]] = [url[k], basin[k]]
    
    return reportDict
#==============================================================================
#     for j in range(len(strName)):    
#         print(strName[j], basin[j])
#==============================================================================
        
""" This is the test code to simply run the rptDict function by itself 
    It is not called when the program is included in another program. """        
#==============================================================================
# foo = rptDict()
# print (foo)
#==============================================================================
