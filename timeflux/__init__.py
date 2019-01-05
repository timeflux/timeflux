""" Timeflux """

__version__ = '0.1.0'

import logging
import coloredlogs

FORMAT = '%(asctime)s    %(levelname)-10s %(module)-12s %(process)-8s %(processName)-16s %(message)s'
LEVEL_STYLES = {'debug': {'color': 'white'}, 'info': {'color': 'cyan'}, 'warning': {'color': 'yellow'}, 'error': {'color': 'red'}, 'critical': {'color': 'magenta'}}
FIELD_STYLES = {'asctime': {'color': 'blue'}, 'levelname': {'color': 'black', 'bright': True}, 'processName': {'color': 'green'}}

logging.basicConfig(format=FORMAT, level=logging.DEBUG)
coloredlogs.install(fmt=FORMAT, level='DEBUG', milliseconds=True, level_styles=LEVEL_STYLES, field_styles=FIELD_STYLES)
