# -*- coding: utf-8 -*-
"""
Created on Wed Oct  12, 2019

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

import os, sys

import datetime
import requests
from bs4 import BeautifulSoup
#import ftplib
import urllib
import configparser

""" Declarations and Parameters from Configuration file"""
config = configparser.ConfigParser()
config.read('./config.ini')

# URLs for data:
hurdatDir = config.get('DOWNLOAD','HURDAT2')
ibtracsDir = config.get('DOWNLOAD','IBTRACS')
ENSOURL = config.get('DOWNLOAD','ENSOURL')
RPTURL = config.get('DOWNLOAD','RPTURL')

# Location and file names to store downloaded data:
dataDir = config.get('DIRECTORIES','DATA')
logDir = config.get('DIRECTORIES','DOWNLOAD_LOG')
if( not os.path.isdir(dataDir) ):
    try:
        os.makedirs(dataDir, exist_ok = True)
    except:
        sys.exit("Creation of data directory failed")
    else:
        print("Data directory successfully created")
else:
    print("Data directory already exists")

# File names
logFile = dataDir + "/dataDownloadHistory.log"
natlFile = dataDir + "/natlData.csv"
nepacFile = dataDir + "/nepacData.csv"
ibtracsFile = dataDir + "/ibtracsData.csv"
nameMappingFile = dataDir + "/nameMapping.txt"
ensoFile = dataDir + "/ensoData.txt"
stormReportFile = dataDir + "/stormreportData.txt"

ibDataPattern = config.get('DOWNLOAD','IBDATAPATTERN')
ibNamesPattern = config.get('DOWNLOAD','IBNAMESPATTERN')

""" Initialize log file """
log  = open(logFile, mode = "a")
msg = str(datetime.datetime.now()) +": Data download\n"
log.write(msg)
#print(msg)
#log.close()
#sys.exit('exit here')

""" IBTrACS download """
result = requests.get(ibtracsDir) #Get the directory list from URL
#print(result.status_code)
pageContent = result.content
#print(pageContent)
soup = BeautifulSoup(pageContent, 'lxml') # Extract the links
links = soup.find_all("a")
#print(links)

for link in links:  # Scan links for needed file names
    if ibDataPattern in link.text:
        ibDataRemote = link.text
    if ibNamesPattern in link.text:
        ibNamesRemote = link.text

log.write("IBTrACS files from " + ibtracsDir + ":\n")
#log.write("  " + ibDataRemote + "\n")
#log.write("  " + ibNamesRemote + "\n") 
# download the files
try:
    urllib.request.urlretrieve(ibtracsDir + "/" + ibDataRemote,ibtracsFile)
except:
    log.write("  Download fail for " + ibDataRemote + "\n")
else:
    log.write("  " + ibDataRemote + "\n")
   
try:
    urllib.request.urlretrieve(ibtracsDir + "/" + ibNamesRemote,nameMappingFile)
except:
    log.write("  Download fail for " + ibNamesRemote + "\n")
else:
    log.write("  " + ibNamesRemote + "\n")
        

""" HURDAT2 Download """

result = requests.get(hurdatDir+"/data")
#result = requests.get("https://www.google.com")
#print(result.status_code)

pageContent = result.content
#print(pageContent)

soup = BeautifulSoup(pageContent, 'lxml')
links = soup.find_all("a")
#print(links)
hurdatFiles = []
for link in links:
    if "download" in link.text:
#    if "About" in link.text:
 #       print(link)
        hurdatFiles.append(link.attrs['href'])

#print(hurdatFiles[0],"\n",hurdatFiles[1])

#NA_data = requests.get(hurdatFiles[0])
log.write("HURDAT2 files from " + hurdatDir + ":\n")
try:
    urllib.request.urlretrieve(hurdatDir + hurdatFiles[0],natlFile)
except:
    log.write("  Download failed for " + hurdatFiles[0] + "\n")
else:
    log.write("  " + hurdatFiles[0] + "\n")

try:
    urllib.request.urlretrieve(hurdatDir + hurdatFiles[1],nepacFile)
except:
    log.write("  Download failed for " + hurdatFiles[1] + "\n")
else:
    log.write("  " + hurdatFiles[1] + "\n")



""" Storm Report Data Download """

try:
    urllib.request.urlretrieve(RPTURL,stormReportFile )
except:
    log.write("  Download failed for " + RPTURL + "\n")
else:
    log.write("  " + RPTURL + "\n")


""" ENSO Data Download """

try:
    urllib.request.urlretrieve(ENSOURL,ensoFile )
except:
    log.write("  Download failed for " + ENSOURL + "\n")
else:
    log.write("  " + ENSOURL + "\n")

log.write("\n")
log.close()
