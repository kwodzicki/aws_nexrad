#!/usr/bin/env python
import logging
import boto3
import os
from datetime import datetime, timedelta

_dateFMT = "%Y%m%d_%H%M%S"
def nexrad_aws_level2( stations, outroot,
        date1      = None,
        date2      = None,
        no_MDM     = True,
        no_tar     = True,
        maxAttempt = 3,
        bucketName = 'noaa-nexrad-level2'):

    if not isinstance( stations, (list,tuple,) ):
        stations = [stations]

    utcnow = datetime.utcnow()
    if (date1 is None):
        date1 = datetime(utcnow.year, utcnow.month, utcnow.day, 0)

    if (date2 is None):
        date2 = datetime(date1.year, date1.month, date1.day+1, 0)
    
    s3conn = boto3.resource('s3');
    bucket = s3conn.Bucket(bucketName)

    date = datetime(date1.year, date1.month, date1.day)
    while date2 > date:
        yyyy       = date.strftime('%Y')
        yyyymm     = date.strftime('%Y%m')
        yyyymmdd   = date.strftime('%Y%m%d')
        
        datePrefix = date.strftime('%Y/%m/%d/')
        
        for station in stations:
            outdir     = os.path.join(outroot, yyyy, yyyymm, yyyymmdd, station)
            if not os.path.isdir( outdir ): os.makedirs( outdir )

            statPrefix = datePrefix + station
            statKeys   = bucket.objects.filter( Prefix = statPrefix )
            for statKey in statKeys:
                fBase = statKey.key.split('/')[-1]
                if (no_MDM and fBase.endswith('MDM')): continue
                if (no_tar and fBase.endswith('tar')): continue
                fDate = datetime.strptime(fBase[4:19], _dateFMT)
                if (fDate >= date1) and (fDate <= date2):
                    localFile = os.path.join(outdir, fBase)
                    if os.path.isfile( localFile ):
                        fSize = os.stat(localFile).st_size
                        if (fSize == statKey.size):
                            continue
                        os.remove(localFile)
                    attempt = 0
                    statObj = statKey.Object()
                    while (attempt < maxAttempt):
                        print('Download attempt {:2d} of {:2d} : {}'.format(
                                attempt+1, maxAttempt, fBase))
                        try:
                            statObj.download_file(localFile)
                        except:
                            attempt += 1
                        else:
                            attempt = maxAttempt
        date += timedelta(days = 1)

if __name__ == "__main__":
    stations = ['KABR', 'KHGX', 'KLWX']
    date1    = datetime(2019,  6,  1,  0, 40)
    date2    = datetime(2019,  6,  1,  1, 20)
    outroot  = '/home/kwodzicki/Data/NEXRAD/level2'
    res      = nexrad_aws_level2(stations, outroot, date1=date1, date2=date2)
