import datetime
import hashlib
import json
import sys
import imp
import re
import os
import subprocess
import base64
import shutil
import ast
import dill

from .classproperty import classproperty,cachedclassproperty

class JSONEncoder(json.JSONEncoder):
    """
    A JSON encoder to support encode datetime
    """
    TZ = datetime.timezone(datetime.timedelta(hours=8),name="Perth")
    def default(self,obj):
        if isinstance(obj,datetime.datetime):
            return {
                "_type":"datetime",
                "value":obj.astimezone(tz=self.TZ).strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
        elif isinstance(obj,datetime.date):
            return {
                "_type":"date",
                "value":obj.strftime("%Y-%m-%d")
            }
        elif obj.__class__.__name__ == 'function':
            return {
                "_type":"function",
                "value":base64.b64encode(dill.dumps(obj)).decode()
            }
        else:
            return json.JSONEncoder.default(self,obj)

class JSONDecoder(json.JSONDecoder):
    """
    A JSON decoder to support decode datetime
    """
    TZ = datetime.timezone(datetime.timedelta(hours=8),name="Perth")
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        from . import timezone
        if '_type' not in obj:
            return obj
        t = obj['_type']
        if t == 'datetime':
            return timezone.nativetime(datetime.datetime.strptime(obj["value"],"%Y-%m-%d %H:%M:%S.%f").replace(tzinfo= self.TZ ))
        elif t == 'date':
            return datetime.datetime.strptime(obj["value"],"%Y-%m-%d").date()
        elif t == 'function':
            return dill.loads(base64.b64decode(obj["value"]))
        else:
            return obj

def env(key, default=None, required=False,vtype=None):
    """
    Retrieves environment variables and returns Python natives. The (optional)
    default will be returned if the environment variable does not exist.
    """
    try:
        value = os.environ[key]
        value = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        pass
    except KeyError:
        if default is not None or not required:
            return default
        raise Exception("Missing required environment variable '%s'" % key)

    if vtype is None:
        if default is not None:
            vtype = default.__class__

    if vtype is None:
        return value
    elif isinstance(value,vtype):
        return value
    elif issubclass(vtype,list):
        if isinstance(value,tuple):
            return list(value)
        else:
            value = str(value).strip()
            if not value:
                return []
            else:
                return value.split(",")
    elif issubclass(vtype,tuple):
        if isinstance(value,list):
            return tuple(value)
        else:
            value = str(value).strip()
            if not value:
                return tuple()
            else:
                return tuple(value.split(","))
    elif issubclass(vtype,bool):
        value = str(value).strip()
        if not value:
            return False
        elif value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        else:
            raise Exception("'{}' is a boolean environment variable, only accept value 'true' ,'false' and '' with case insensitive, but the configured value is '{}'".format(key,value))
    elif issubclass(vtype,int):
        return int(value)
    elif issubclass(vtype,float):
        return float(value)
    else:
        raise Exception("'{0}' is a {1} environment variable, but {1} is not supported now".format(key,vtype))

def file_md5(f):
    cmd = "md5sum {}".format(f)
    output = subprocess.check_output(cmd,shell=True)
    return output.split()[0].decode()

def remove_file(f):
    if not f: 
        return

    try:
        os.remove(f)
    except:
        pass

def remove_folder(f):
    if not f: 
        return

    try:
        shutil.rmtree(f)
    except:
        pass

def file_size(f):
    return os.stat(f).st_size
