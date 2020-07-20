import pytz
import os
import logging.config

from dotenv import load_dotenv
from ..utils import env
from datetime import datetime


HOME_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dot_env = os.path.join(HOME_DIR, ".env")
load_dotenv(dotenv_path=dot_env)

DEBUG = env("DEBUG",False)

TIME_ZONE = env("TIME_ZONE",'Australia/Perth')
TZ = datetime.now(tz=pytz.timezone(TIME_ZONE)).tzinfo

AZURE_CONNECTION_STRING = env("TEST_STORAGE_CONNECTION_STRING",vtype=str,required=True)
AZURE_CONTAINER = env("TEST_CONTAINER",vtype=str,required=True)
RESOURCE_NAME = env("TEST_RESOURCE_NAME",vtype=str,required=True)

LOCAL_STORAGE_ROOT_FOLDER = env("TEST_STORAGE_ROOT_FOLDER",vtype=str,required=True)

logging.basicConfig(level="WARNING")

LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        #'console': {'format':  '%(asctime)s %(levelname)-8s %(name)-15s %(message)s'},
        'console': {'format':  '%(asctime)s %(levelname)-8s %(message)s'},
    },
    'handlers': {
        'console': {
            'level': 'DEBUG' if DEBUG else 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
    },
    'loggers': {
        'data_storage': {
            'handlers': ['console'],
            'level': 'INFO' if DEBUG else 'WARNING',
            'propagate':False
        },
        'data_storage.unittest': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'WARNING',
            'propagate':False
        },
    },
    'root':{
        'handlers': ['console'],
        'level': 'WARNING',
        'propagate':False
    }
}
logging.config.dictConfig(LOG_CONFIG)
