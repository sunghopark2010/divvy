# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 18:35:49 2014
"""
# preliminary analysis and aggregation
from __future__ import division
import sys

# scientific libraries
import numpy as np  # NumPy (multidimensional arrays, linear algebra, ...)
import scipy as sp  # SciPy (signal and image processing library)
import pandas as pd # Pandas
import pydot

from pandas import DataFrame, Series
from scipy import stats
#from sklearn import tree
#from sklearn.externals.six import StringIO

import matplotlib as mpl         # Matplotlib (2D/3D plotting library)
import matplotlib.pyplot as plt  # Matplotlib's pyplot: MATLAB-like syntax
from matplotlib.backends.backend_pdf import PdfPages
from pylab import *              # Matplotlib's pylab interface
ion()                            # Turned on Matplotlib's interactive mode

import requests, zipfile, StringIO
import itertools

from sklearn import linear_model

from datetime import date as dt
from datetime import timedelta
from datetime import datetime

from functools import partial
from collections import namedtuple

chain = itertools.chain
combinations = itertools.combinations


def replaceNA(param, row):
    if param.keycol == '':
        for c_ in param.nacol:
            row[c_] = param.naval
    else:
        if row[param.keycol] == param.keyval:
            for c_ in param.nacol:
                row[c_] = param.naval
    return row


def parseDatetime(dateformat, dttime):
    return datetime.strptime(dttime, dateformat)
    
    
def degreeToRadius(degree):
    return degree * np.pi / 180


def distbyLongLat(lat1, lat2, long1, long2):
    earthr = 6371
    dlon = degreeToRadius(long1 - long2)
    dlat = degreeToRadius(lat1 - lat2)
    
    rlat1 = degreeToRadius(lat1)
    rlat2 = degreeToRadius(lat2)

    var = sin(dlat/2) ** 2 + cos(rlat1)*cos(rlat2)*sin(dlon/2)**2
    dist = earthr * 2 * arctan2(sqrt(var), sqrt(1-var)) * 0.621371
    return dist
    

def velocity(distance, duration):
    try:
        return distance / (duration/(60*60))
    except:
        return 0



def getHour(param, row):
    starttime = parseDatetime(param.dateformat, row[param.timeattrib])
    return starttime.hour
    

def getDate(param, row):
    starttime = parseDatetime(param.dateformat, row[param.timeattrib])
    return starttime.strftime('%m%d%H%M')


def getDistbyLongLat(param, row):
    return distbyLongLat(row[param.fromlocation.latitude],
                         row[param.tolocation.latitude],
                         row[param.fromlocation.longitude],
                         row[param.tolocation.longitude])
    
def getVelocity(row):
    return velocity(row['distance'], row['tripduration'])
    
    
def joinData(tripsData, stationData, onkey, latName, lonName):
    tripsData = tripsData.join(stationData, 
                               on=onkey)
    scol = Series(tripsData.columns)
    scol[scol=='latitude'] = latName
    scol[scol=='longitude'] = lonName
    tripsData.columns = scol   
    return tripsData


def groupAge(row):
    if row['age'] < 20:
        return 1
    if row['age'] < 30:
        return 2
    if row['age'] < 40:
        return 3
    if row['age'] < 50:
        return 4
        


def convertToFloat(val):
    try:
        return float(val.replace(',',''))
    except:
        return val


if __name__ == '__main__':
    stationData = pd.read_csv('./data/Divvy_Stations_2013.csv')
    tripsData = pd.read_csv('./data/Divvy_Trips_2013.csv')
    
    tripsData = tripsData.applymap(convertToFloat)
    
    
    today = dt.today()
    thisyear = today.year
    tripsData['birthyear'] = thisyear - tripsData['birthday']
    scol = Series(tripsData.columns)
    scol[scol=='birthyear'] = 'age'
    tripsData.columns = scol
    
        
    # make parameters
    paramStruct = namedtuple('paramStruct', 
                                 'nacol, naval, keycol, keyval, \
                                  dateformat, timeattrib, \
                                  colToRemove, colToRemove2, \
                                  fromlocation, tolocation, \
                                  groupkey, groupkey2')
    paramLocStruct = namedtuple('paramLocStruct',
                                    'station, latitude, longitude')
    param = paramStruct(
        nacol =['gender', 'age'], 
        naval = 0, 
        keycol = 'usertype', 
        keyval = 'Customer',
        dateformat = '%m/%d/%Y %H:%M',
        timeattrib = 'starttime',
        colToRemove = 
            ['starttime','stoptime', 'bikeid', 'trip_id', 'usertype', 
             'from_station_id', 'to_station_id', 'to_station_name', 'from_station_name'],
        colToRemove2 = ['from_lat', 'from_lon', 'to_lat', 'to_lon'],
        fromlocation = paramLocStruct(station = 'from_station_name', latitude = 'from_lat', longitude = 'from_lon'),
        tolocation = paramLocStruct(station = 'to_station_name', latitude = 'to_lat', longitude = 'to_lon'),
        groupkey = ['gender', 'age', 'hour', 'date'],
        groupkey2 = ['gender', 'age', 'hour', 'date', 'from_lat', 'from_lon', 'to_lat', 'to_lon'],
        )
    
    replacefunc = partial(replaceNA, param)
    tripsData = tripsData.apply(replacefunc, axis=1)  # replace na with cust

    dtfunc = partial(getDate, param)
    datedate = DataFrame(tripsData.apply(dtfunc, axis=1), columns=['date'])
    

    stationData.index = stationData['name']
    
    tripsData = joinData(tripsData, stationData[['latitude', 'longitude']], 
                         param.fromlocation.station, 
                         param.fromlocation.latitude, 
                         param.fromlocation.longitude)
    tripsData = joinData(tripsData, stationData[['latitude', 'longitude']], 
                         param.tolocation.station, 
                         param.tolocation.latitude, 
                         param.tolocation.longitude)
    
    distfunc = partial(getDistbyLongLat, param)
    dist = DataFrame(tripsData.apply(distfunc, axis=1), columns = ['distance'])
    tripsData = pd.concat([hour, datedate, dist, tripsData], axis=1)

    veloc = DataFrame(tripsData.apply(getVelocity, axis=1), columns = ['velocity'])
    tripsData = pd.concat([veloc, tripsData], axis=1).drop(param.colToRemove, axis=1)
    
    
    tripsData['age'] = tripsData['age'].fillna(0)
    tripsData['age'] = tripsData['age'].replace('cust', 0)
    tripsData['gender'] = tripsData['gender'].replace([np.nan, 'cust'], 0)
    tripsData['gender'] = tripsData['gender'].replace('Male', 1)
    tripsData['gender'] = tripsData['gender'].replace('Female', 2)
    
    tripsData['age'] = DataFrame(tripsData['age']/10).applymap(floor)
    
    output = tripsData.groupby(param.groupkey2).mean()
    output.to_csv('output2.csv')
    
    tripsData = tripsData.drop(param.colToRemove2, axis=1)
    output2 = tripsData.groupby(param.groupkey).mean()
    output2.to_csv('output.csv')