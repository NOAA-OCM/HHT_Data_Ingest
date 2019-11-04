# Getting Started

This document describes how to get started with running this code on your own computer. 


## Python Environment Setup
1. Make sure `python 3.7` is installed
    ```bash
        python --version
    ```
2. Update pip to latest 
    ```bash
        sudo -H pip3 install --upgrade pip
    ```

## Create Virtual environment so we don't muck up our system

    All the commands below are to be executed in the root of this code directory

1. Install Virtual Env 
    ```bash
        sudo pip install virtualenv
    ```
3. Create a new Virtual Env for this project 
    ```bash
        virtualenv -p python3 venv
    ```
4. Switch to using our new virtual environment a.k.a Safe Bubble a.k.a Don't Mess Up Your System
    
    On Linux or OSX
    ```bash
        source venv/bin/activate
    ```
    On Windows run
    ```
        venv\Scripts\activate.bat
    ```

## Install Library Dependencies

This project comes with a `requirements.txt` file. This can be used to install all the dependencies with the recommended version. You can skip the next step after this.

```bash
    pip install --no-cache-dir -r requirements.txt
```

## Create or Update your own `requirements.txt` (optional)

Skip the last step of installing dependencies from `requirements.txt` if you want to create your own or update any dependencies. In which case right after the fresh virtual env install follow these steps

1. Install Library Dependencies
    ```bash
        pip3 install beautifulsoup4
        pip3 install requests
        pip3 install lxml
        pip3 install pyshp
    ```
2. Now Freeze these dependencies into `requirements.txt`
    ```bash
        pip freeze > requirements.txt
    ```

## Make your own hurricanes tracks and segments datasets

1. Download and create raw datasets
    ```bash
        python3 downloadHurricaneData.py        
    ```

    This script will create a new directory called `data` in the project root with following output files. These files will be used as input to the next script.

    ```
        $ ls -l data
            dataDownloadHistory.log
            ensoData.txt  
            ibtracsData.csv  
            nameMapping.txt  
            natlData.csv  
            nepacData.csv  
            stormreportData.txt
    ```
    
2. Create Final Datasets
    ```bash
        python3 annualDataUpdate.py        
    ```

    This script will create the following output datasets in `results` directory
    ```bash
        $ ls -l results
            hurricaneYears.json
            Segments_WebMerc.dbf
            Segments_WebMerc.prj
            Segments_WebMerc.shp
            Segments_WebMerc.shx
            stormnames.js
            Tracks_WebMerc.dbf
            Tracks_WebMerc.prj
            Tracks_WebMerc.shp
            Tracks_WebMerc.shx
            update.log
    ```

## Finally to exit your virtual environment

On Linux or OSX
```bash
    source venv/bin/activate
```
On Windows run
```
    venv\Scripts\deactivate.bat
```