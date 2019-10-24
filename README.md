# README #

This repository contains the programs and some test data used for updating the Historical Hurricane Tracks (HHT) web site, [https://coast.noaa.gov/hurricanes/](https://coast.noaa.gov/hurricanes/).  These programs will ingest and combine three different data sources: two HURDAT2 data files, one each for the North Atlantic and one for the  Northeastern Pacific, from the National Hurricane Centers [HURDAT2 data sets](https://www.nhc.noaa.gov/data/#hurdat), and one global data file from the [International Best Track Archive for Climate Stewardship (IBTrACS)](https://www.ncdc.noaa.gov/ibtracs/) data set, currently (September, 2019) at version [V04r00](https://www.ncdc.noaa.gov/ibtracs/index.php?name=ib-v4-access).  

The program now uses a configuration file, `config.ini` to set up all directories, URLs, etc.  In theory, this should be the only file that needs updating each year.  Change it as the first step in the update process. All data sets needed are then downloaded in step 2, with the `downloadHurricaneData.py` program.  They are all saved in a data directory specified in the configuration file.  Step 3 is to run `annualDataUpdate.py` program, which now creates just two different shapefiles,  a Tracks shapefile, containing one attributed polyline for each storm, and a Segments shapefile, containing many polylines per storm, where each line represents the track segment from one time observation up to the next one.  Storms that are missing all information about wind speed and Minimum Pressure are entered with all other storms.

In the current (Oct. 2019) HHT data update process, these shapefiles are then loaded into an SQL database of the Tracks and Segments. That database is what is actually used by the HHT web application.  

Version 3, when released will use a PostgreSQL database instead fo relying on shapefiles.


For additional information, contact:  
Dave Eslinger  
NOAA Office for Coastal Management  
dave.eslinger@noaa.gov

## NOAA Open Source Disclaimer
This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ?as is? basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.

## License
Software code created by U.S. Government employees is not subject to copyright in the United States (17 U.S.C.
�105). The United States/Department of Commerce reserve all rights to seek and obtain copyright protection in
countries other than the United States for Software authored in its entirety by the Department of Commerce. To
this end, the Department of Commerce hereby grants to Recipient a royalty-free, nonexclusive license to use,
copy, and create derivative works of the Software outside of the United States.
