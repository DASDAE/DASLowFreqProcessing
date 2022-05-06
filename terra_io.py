import dascore
from dascore.core import Patch
from glob import glob
from datetime import datetime
import h5py
from . import spool
from imp import reload
reload(spool)
from dascore.constants import REQUIRED_DAS_ATTRS
from dascore.io.terra15.core import Terra15Formatter
import numpy as np
import pandas as pd
import sys

def print_progress(n):
    sys.stdout.write("\r" + str(n))
    sys.stdout.flush()

def create_dataset(datapath):
    datafiles = glob(datapath+'/*.hdf5')
    bgtimes = []
    edtimes = []
    for i,filename in enumerate(datafiles[:]):
        print_progress(filename)
        with h5py.File(filename,'r') as f:
            bt = f['velocity/gps_time'][0]
            et = f['velocity/gps_time'][-1]
        bt = np.datetime64(datetime.utcfromtimestamp(bt))
        et = np.datetime64(datetime.utcfromtimestamp(et))
        bgtimes.append(bt)
        edtimes.append(et)
    df = pd.DataFrame()
    df['file'] = datafiles[:]
    df['start_time'] = bgtimes
    df['end_time'] = edtimes
    df = df.sort_values(by='start_time')
    # remove last file
    df = df.iloc[:-1]
    return df

def terra_reader(filename, bgtime, edtime):
    st = Terra15Formatter().read(filename,time=(bgtime,edtime))
    return st[0]

def create_spool(datapath):
    df = create_dataset(datapath)
    sp = spool.spool(df,terra_reader,support_partial_reading=True)
    return sp