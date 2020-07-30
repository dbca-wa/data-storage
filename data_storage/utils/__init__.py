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
import logging
import traceback

from .classproperty import classproperty,cachedclassproperty

logger = logging.getLogger(__name__)

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

def _get_simple_property(obj,prop_name):
    if isinstance(obj,(list,tuple)):
        try:
            prop_name = int(prop_name)
            if prop_name < len(obj):
                return obj[prop_name]
        except:
            raise Exception("Please use integer index({}) to access a list.".format(prop_name))
    elif isinstance(obj,dict):
        return obj.get(prop_name)
    elif hasattr(obj,prop_name):
        return getattr(obj,prop_name)
    else:
        return None


def get_property(obj,prop_name,convert_func=None,default=None,multi_properties=False):
    """
    Get property value from object(dict object or normal object)
    prop_name: 
        a string for simple property 
        tuple or "." separated string for nested property
        a list of (string or tuple or "." separated string) for alternative property
    multi_properties:only meaningful if prop_name is a list of properties;
        find the first non-None property if it is False;
        find the value of all the properties if it is True
    convert_fuc: convert the data . 
       it takes the property value as the only argument, if prop_name is a single property or is a list of properties but multi_properties is False
       it takes the list of values of the properties if prop_name is the list of properties and multi_properties if True
    Return 
        if value is None, return default value
        If convert_func is not None, return te result of the convert_func
        otherwise:
            return the value , if prop_name is a single property or is a list of properties but multi_properties is False
            return the list of value , if prop_name is a list of properties but multi_properties is True
    """
    if not obj:
        val = None
        multi_properties = False
    elif not prop_name:
        val = obj
        multi_properties = False
    elif isinstance(prop_name,tuple):
        val = obj
        for name in prop_name:
            val = _get_simple_property(val,name)
            if val is None:
                break
        multi_properties = False
    elif isinstance(prop_name,list):
        if len(prop_name) == 1:
            return get_property(obj,prop_name[0],convert_func=convert_func)
        else:
            if multi_properties:
                val = []
                for p_name in prop_name:
                    val.append(get_property(obj,p_name))
            else:
                val = None
                for p_name in prop_name:
                    val = get_property(obj,p_name)
                    if val is not None:
                        break
    elif "." in prop_name:
        return get_property(obj,tuple(prop_name.split(".")),convert_func=convert_func)
    else:
        val = _get_simple_property(obj,prop_name)
        multi_properties = False

    if val is None:
        if default is None:
            return convert_func(None) if convert_func else None
        else:
            return default 
    else:
        if callable(val):
            val = val()

        if multi_properties:
            return convert_func(*val) if convert_func else val
        else:
            return convert_func(val) if convert_func else val

