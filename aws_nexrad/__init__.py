import logging
import os, json

log = logging.getLogger(__name__);
log.setLevel(logging.DEBUG)

from .download_nexrad_level2 import download_nexrad_level2


# Read in station IDs from json file
_fileDir  = os.path.dirname( os.path.realpath(__file__) )
_statJSON = os.path.join(_fileDir, 'data', 'nexrad_station_id_list.json')
with open(_statJSON, 'rb') as _fid:
    _stations = json.load(_fid)
    for _key, _val in _stations.items():
        globals()[_key] = _val
