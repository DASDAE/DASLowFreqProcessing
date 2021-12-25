

import sys
from dascore.core import Patch
from collections import OrderedDict

class spool:
    """
    spool class to handle dataset on storage.
    A pandas DataFrame and a callable reader function is required 
    to construct the class

    The DataFrame should have three columns:
        file: file name including relative path
        start_time: starting time of the file
        end_time: ending time of the file
    
    reader is the function to load data file into Patch
        if reader does not support partial reading:
        patch = reader(filename) 
        if reader supports partial reading:
        patch = reader(filename,start_time=None, end_time=None) 
    
    Example:
        sp = spool(df,reader,support_partial_reading=False)
    """

    def __init__(self,df = None,
                reader = None,
                support_partial_reading = False):
        self._df = df
        self._reader = reader
        self._partial_reading = support_partial_reading
        self._cashe_size_limit = 1.0 # in GB
        self._cashe = OrderedDict({})
        pass

    def set_database(self,df):
        df = df.sort_values(by='start_time')
        self._df = df

    def set_reader(self,reader,support_partial_reading = False):
        """
        set reader function to load data file into Patch
            if reader does not support partial reading:
            patch = reader(filename) 
            if reader supports partial reading:
            patch = reader(filename,start_time=None, end_time=None) 
        Usage:
            sp.set_reader(reader,support_partial_reading = False):
        """
        self._reader = reader
        self._partial_reading = support_partial_reading
    
    def set_cashe_size(self,s):
        """
        Set cashe size limit (in GB) for spool.
        """
        self._cashe_size_limit = s
    
    def _estimate_cashe_size(self):
        s = 0.0
        for p in self._cashe.values():
            s += sys.getsizeof(p.data)
        s = s/1024**3  # convert to GB
        return s
    
    def _load_to_cashe(self,ind):
        filename = self._df['file'].iloc[ind]
        # check whether file is in cashe already
        if filename in self._cashe.keys():
            return False
        patch = self._reader(filename)
        self._cashe[filename] = patch
        # remove old data until cashe is smaller than limit
        while (self._estimate_cashe_size() > self._cashe_size_limit) \
            & (len(self._cashe)>1):
            print('pop')
            self._cashe.popitem(last=False)
        return True
    
    def _get_data_nopl(self,bgtime,edtime):
        """
        function to load data without partial loading support
        """


