import logging

log = logging.getLogger(__name__);
log.setLevel(logging.DEBUG)

from .nexrad_aws_level2 import nexrad_aws_level2
