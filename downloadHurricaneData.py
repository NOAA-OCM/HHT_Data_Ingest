# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 11:52:42 2019

@author: Dave Eslinger
         dave.eslinger@noaa.gov
         
This program downloads HURDAT2 data for the North Atlantic and Northeastern
Pacific from the NOAA/NWS National Hurricane Center HURDAT2 data sets at:
    https://www.nhc.noaa.gov/data/#hurdat
    
It also downloads IBTrACS data (one file with global coverage) and a file
with a crosswalk between IBTrACS ID names and those used by the contributing
agencies.  We need this to give HURDAT2-derived storms the correct identifier
to use with the Storm_Details link hosted by IBTrACS. IBTrACS data at:
    https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/
    
Because links change at least yearly, and frequently more often, this program 
first does a bit of web scrapping/directory listing to find all potential 
files and then identify the correct links based on some pattern matching.
"""

import os
import requests
from bs4 import BeautifulSoup
import ftplib
import urllib

""" Data locations, filenames, etc. """
# URLs for data:
hurdatDir = "https://www.nhc.noaa.gov"
ibtracsDir = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/'
#print(ibtracsDir)

# Location and file names to store downloaded data:
dataDir = "./data"
if( not os.path.isdir(dataDir) ):
    try:
        os.mkdir(dataDir)
    except:
        print("Creation of data directory failed")
    else:
        print("Data directory successfully created")
else:
    print("Data directory already exists")

natlFile = dataDir + "/natlData.csv"
nepacFile = dataDir + "/nepacData.csv"
ibtracsFile = dataDir + "/ibtracsData.csv"

result = requests.get(hurdatDir+"/data")
#result = requests.get("https://www.google.com")
#print(result.status_code)

src = result.content
#print(src)

soup = BeautifulSoup(src, 'lxml')
links = soup.find_all("a")
#print(links)
hurdatFiles = []
for link in links:
    if "download" in link.text:
#    if "About" in link.text:
 #       print(link)
        hurdatFiles.append(hurdatDir + link.attrs['href'])

print(hurdatFiles[0],"\n",hurdatFiles[1])

#NA_data = requests.get(hurdatFiles[0])
urllib.request.urlretrieve(hurdatFiles[0],natlFile)
urllib.request.urlretrieve(hurdatFiles[1],nepacFile)