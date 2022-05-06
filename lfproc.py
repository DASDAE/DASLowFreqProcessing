
from xarray.core.utils import FrozenDict
from .spool import spool
import numpy as np
import pandas as pd
import os
import shutil
from datetime import datetime
from dascore.utils.patch import merge_patches
from dascore.core import Patch
from glob import glob
import matplotlib.pyplot as plt

class lfproc:

    def __init__(self,sp=None):
        self._spool = sp
        self._para = self._default_process_parameters()
        self._output_folder = None
    
    def set_output_folder(self,folder,delete_existing=False):
        self._output_folder = folder
        if delete_existing and os.path.isdir(folder):
            shutil.rmtree(folder)
            print(f'original {folder} deleted')
        if not os.path.isdir(folder):
            os.mkdir(folder)
            print(f'{folder} created')
    
    def _default_process_parameters(self):
        para = {'output_sample_interval': 1.0, # in seconds
                'process_patch_size':100, # in output sample interval
                'edge_buff_size':10,  # in output sample interval
                'data_gap_tolorance':10.0
        }
        return para

    def update_processing_parameter(self,**kwargs):
        for key, value in kwargs.items():
            if key not in self._para.keys():
                print(f'{key} is not default parameter key')
            else:
                self._para[key]=value
        return self.parameters
    
    def process_time_range(self,bgtime,edtime):
        if self._output_folder is None:
            raise Exception('Please setup output folder first')
        dt = self._para['output_sample_interval']
        patch_size = self._para['process_patch_size']
        buff_size = self._para['edge_buff_size']

        time_grid = (np.arange(bgtime.astype('datetime64[ns]')
                ,edtime.astype('datetime64[ns]'),
                np.timedelta64(int(dt*1000),'ms')))
        
        if len(time_grid)<=patch_size:
            patch_size = len(time_grid)-1
        
        def lp_process(DASdata,bgind, edind):
            # low pass filter and downsampling
            lfDAS = DASdata.pass_filter(time=(None,1/dt/2*0.9))\
                        .interpolate(time=time_grid[bgind:edind])
            lfDAS = lfDAS.update_attrs(d_time=dt)
            # output the result to output folder
            filename = _get_filename(lfDAS.attrs['time_min'],
                        lfDAS.attrs['time_max'])
            filename = self._output_folder + '/' + filename
            lfDAS.save_pickle(filename)
        
        # load and process the first patch
        plist = self._spool.get_patch(time_grid[0],time_grid[patch_size])
        DASdata = _check_merge(plist)
        # low pass filter and downsampling
        lp_process(DASdata,buff_size,patch_size-buff_size)

        # define the processing flow to avoid repeat code
        def merge_and_process(DASdata):
            plist = self._spool.get_patch(time_grid[data_end],time_grid[new_data_end])
            newdata = _check_merge(plist)
            # merge with existing one
            DASdata = DASdata.select(time=(time_grid[data_end-2*buff_size],None))
            plist = merge_patches((DASdata,newdata),tolerance=10.0)
            DASdata = _check_merge(plist)
            # low pass filter and down sample
            lp_process(DASdata,data_end-buff_size,new_data_end-buff_size)
            return DASdata

        # processing for the rest of the dataset
        data_end = patch_size
        new_data_end = data_end + patch_size-2*buff_size
        while new_data_end < len(time_grid):
            # readin new data
            DASdata = merge_and_process(DASdata)
            # update index
            data_end = new_data_end
            new_data_end = data_end + patch_size-2*buff_size
        
        # dealing with the rest of data smaller than patch_size
        if (len(time_grid)-data_end)>1:
            new_data_end = len(time_grid)-1
            DASdata = merge_and_process(DASdata)
    
    ### property definiations    

    @property
    def parameters(self):
        return FrozenDict(self._para)

# end of class lfproc

def gather_results(folder):
    files = glob(folder+'/LFDAS_*.p')
    plist = []
    for file in files:
        d = Patch()
        d.load_pickle(file)
        plist.append(d)
    return merge_patches(plist)

def _check_merge(plist):
    if len(plist)>1:
        print(plist)
        raise Exception('patch merge failed! Gap in data exists')
    else:
        return plist[0]

def _get_timestr(bgtime:np.datetime64) -> str:
    timestr = str(bgtime.astype('datetime64[ms]'))[:21]
    timestr = timestr.replace(':','') # for windows compatable
    return timestr
    
def _get_filename(bgtime,edtime) -> str:
    filename = 'LFDAS_' + _get_timestr(bgtime)\
                + '_' + _get_timestr(edtime) + '.p'
    return filename

def plot_results(plist,cx=np.array([-1,1])):
    for p in plist:
        pm = p._data_array.plot(cmap='seismic',yincrease=False
                ,vmin=cx[0],vmax=cx[1],ax=plt.gca(),add_colorbar=False)
    plt.colorbar(pm)
    plt.xlim(plist[0].attrs['time_min'],plist[-1].attrs['time_max'])
