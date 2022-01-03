from obspy.io.segy.core import _read_segy
from obspy.io.segy.segy import iread_segy
import numpy as np
import pandas as pd
from dascore.core import Patch
from glob import glob
from .spool import spool
from datetime import datetime

def read_segy(file): 
    
    tr = _read_segy(file)

    data = []
    for t in tr.traces:
        data.append(t.data)
    data = np.array(data)

    taxis = np.array([
                    np.timedelta64(int(t*1e6),'us') + np.datetime64('1970-01-01T00:00:00')
                    for t in tr.traces[0].times('timestamp')
                ])

    daxis = np.arange(len(tr.traces))*1.0

    taxis.shape

    DASdata = Patch(data,coords={'distance':daxis,'time':taxis})
    
    DASdata = DASdata.update_attrs(d_time=tr.traces[0].stats['delta'])
    DASdata = DASdata.update_attrs(d_distance=1.0)
    
    return DASdata

def create_dataset_byfilename(datapath):
    files = glob(datapath+'/*.sgy')
    ind = files[0].find('2015')
    bgtimes = np.array([np.datetime64(datetime.strptime(f[ind:ind+15],'%Y%m%d_%H%M%S'))
                for f in files])
    edtimes = bgtimes + np.timedelta64(30,'s') - np.timedelta64(500,'us')

    df = pd.DataFrame()
    df['file'] = files
    df['start_time'] = bgtimes
    df['end_time'] = edtimes
    df = df.sort_values(by='start_time')

    return df

def create_dataset(datapath):
    files = glob(datapath+'/*.sgy')
    bgtimes = []
    edtimes = []
    for f in files:
        bt,et = get_time_range(f)
        bgtimes.append(bt)
        edtimes.append(et)

    df = pd.DataFrame()
    df['file'] = files
    df['start_time'] = bgtimes
    df['end_time'] = edtimes
    df = df.sort_values(by='start_time')
    return df

def create_spool(datapath):
    df = create_dataset(datapath)
    sp = spool(df,read_segy,support_partial_reading=False)
    return sp


def timestamp_to_datetime64(ts):
    return np.timedelta64(int(ts*1e6),'us') + np.datetime64('1970-01-01T00:00:00')

def get_time_range(file):
    s =  iread_segy(file)
    trace = next(s)
    bgtime = timestamp_to_datetime64(trace.times('timestamp')[0])
    edtime = timestamp_to_datetime64(trace.times('timestamp')[-1])
    return bgtime,edtime
