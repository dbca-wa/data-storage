from datetime import datetime as pdatetime

from data_storage import settings


def now():
    """
    Return the current time with configured timezone
    """
    return pdatetime.now(tz=settings.TZ)

def datetime(year,month=1,day=1,hour=0,minute=0,second=0,microsecond=0):
    return pdatetime(year,month,day,hour,minute,second,microsecond,tzinfo=settings.TZ)

def nativetime(d=None):
    """
    Return the datetime with configured timezone, 
    if d is None, return current time with configured timezone
    if d is not None and d.tzinfo is not None, return the datetime with configured timezone
    if d is not None and d.tzinfo is None, set the datetime's timezone to configured timezone
    """
    if d:
        if d.tzinfo:
            return d.astimezone(settings.TZ)
        else:
            return d.replace(tzinfo=settings.TZ)
    else:
        return now()

