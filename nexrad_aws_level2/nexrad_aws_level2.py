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
    """
    Name:
        nexrad_aws_level2
    Purpose:
        Function for downloading NEXRAD Level 2 data from AWS.
    Inputs:
        None.
    Keywords:
        stations   : Scalar string or list of strings containing 
                        radar station IDs in the for KXXX
        outroot    : Top level output directory for downloaded files.
                        This function will create the following directory
                        structure in the directory:
                            <outroot>/YYYY/YYYYMM/YYYYMMDD/KXXX/
        date1      : datetime object with starting date/time for
                        data to download.
                            DEFAULT: Current UTC day at 00Z
        date2      : datetime object with ending date/time for
                        data to download.
                            DEFAULT: End of current UTC day
        no_MDM     : Set to True to exclude *_MDM files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        no_tar     : Set to True to exclude *tar files from
                        download. THIS IS THE DEFAULT BEHAVIOR
        maxAttempt : Maximum number of times to try to download
                        file. DEFAULT: 3
        bucketName : Name of the AWS s3 bucket to download data
                        from. DEFAULT: 'noaa-nexrad-level2'
    Author and History:
        Kyle R. Wodzicki     Created 2019-07-06
    """
    log = logging.getLogger(__name__)
    if not isinstance( stations, (list,tuple,) ): stations = [stations];            # If stations is not an iterable, assume is string and make iterable

    utcnow = datetime.utcnow();                                                     # Get current UTC time
    if (date1 is None):
        date1 = datetime(utcnow.year, utcnow.month, utcnow.day, 0);                 # Set default start date to current UTC day at 00Z

    if (date2 is None):
        date2 = datetime(date1.year, date1.month, date1.day+1, 0);                  # Set default end date to tomorrow's UTC day at 00z
    
    s3conn = boto3.resource('s3');                                                  # Start client to AWS s3
    bucket = s3conn.Bucket(bucketName);                                             # Connect to bucket

    nTot   = 0
    nSuc   = 0
    nFail  = 0
    date   = datetime(date1.year, date1.month, date1.day, 0);                         # Get start date at 00 hour; used to generate file keys, which are base on year/month/day
    while date2 > date:                                                             # While the end date is greater than date
        yyyy       = date.strftime('%Y');                                           # Get 4 digit year
        yyyymm     = date.strftime('%Y%m');                                         # Get 4 digit year and zero padded 2 digit month
        yyyymmdd   = date.strftime('%Y%m%d');                                       # Get 4 digit year, zero padded 2 digit month, and zero padded 2 digit day
        
        datePrefix = date.strftime('%Y/%m/%d/');                                    # Set date prefix for key filtering of bucket
        
        for station in stations:                                                    # Iterate over all stations in the station list
            outdir     = os.path.join(outroot, yyyy, yyyymm, yyyymmdd, station);    # Build local output directory path 
            if not os.path.isdir( outdir ): os.makedirs( outdir );                  # If the output diretory does NOT exist, create it

            statPrefix = datePrefix + station;                                      # Create station prefix for bucket filter using datePrefix and the station ID
            statKeys   = bucket.objects.filter( Prefix = statPrefix );              # Apply filter to bucket objects
            for statKey in statKeys:                                                # Iterate over all the objects in the filter
                fBase = statKey.key.split('/')[-1];                                 # Get the base name of the file
                if (no_MDM and fBase.endswith('MDM')): continue;                    # If the no_MDM keyword is set and the file ends in MDM, then skip it
                if (no_tar and fBase.endswith('tar')): continue;                    # If the no_tar keyword is set and the file ends in tar, then skip it
                fDate = datetime.strptime(fBase[4:19], _dateFMT);                   # Create datetime object for file using information in file name
                if (fDate >= date1) and (fDate <= date2):                           # If the date/time of the file is within the date1 -- date2 range
                    localFile = os.path.join(outdir, fBase);                        # Create local file path
                    if os.path.isfile( localFile ):                                 # If the local file already exists
                        fSize = os.stat(localFile).st_size;                         # Get the file's size
                        if (fSize == statKey.size):                                 # If the local file's size matches that of the remote file
                            log.debug('Already downloaded : {}'.format(fBase) );    # Log debug info
                            continue;                                               # It has already been downloaded, so skip it
                        os.remove(localFile);                                       # If made here, then file sizes do NOT match, so delete local file
                    nTot    += 1
                    attempt  = 0;                                                   # Set attempt to zero
                    statObj  = statKey.Object();                                    # Get Object, need to do this to use the .download_file() method
                    while (attempt < maxAttempt):                                   # While we have not reached maximum attempts
                        print('Download attempt {:2d} of {:2d} : {}'.format(
                                attempt+1, maxAttempt, fBase));                     # Log some info
                        try:
                            statObj.download_file(localFile);                       # Try to download the file
                        except:
                            attempt += 1;                                           # On exception, increment attempt counter
                        else:
                            attempt = maxAttempt + 1;                               # Else, file donwloaded so set attempt to 1 greater than maxAttempt

                    if (attempt == maxAttempt):                                     # If the download attempt matches maximum number of attempts,then all attempts failed
                        nFail += 1
                        log.error('Failed to download : {}'.format(fBase) );        # Log error
                    else:
                        nSuc += 1
        date += timedelta(days = 1);                                                # Increment date by one (1) day

    return nTot, nSuc, nFail
