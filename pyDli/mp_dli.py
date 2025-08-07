import concurrent.futures
import sys
from copy import deepcopy
from numpy import arange 
import json
import pickle
from pathlib import Path
import logging
from pyDli.pyDli import PyDli
import shutil


class MP_dli(): 
    def __init__(self, version : str, path : str, stream_label: str, tsRange =None, 
                 block_size : int = 50000, max_workers = 32, output_path=None, output_type='pk'):
        
        self.version = version
        self.path = path
        self.stream_label = stream_label
        self.block_size = block_size
        self.tsRange = tsRange
        self.max_workers = max_workers
        self.output_path = output_path
        self.output_type = output_type
        
    def get_tsrange(self): 
        dli = PyDli(caseDir=self.path, dliVersion=self.version)
        dli.loadDli()
        dli.find_stream_path(self.stream_label, search_path=self.path)
        return list(dli.DliReader.GetFirstLastKey(str(dli.stream_path))) 

    def worker(self, dli, stream_label, ts, block_size): 
        dli.loadDli()
        dli.find_stream_path(self.stream_label, search_path=self.path)
        trace = dli.read(stream_label=stream_label, ts_range=[int(ts), int(ts+block_size)])
        res_dict = dli.parse(trace, self.stream_label)

    
        
        if self.output_path: 
            if self.output_type == 'json':
                output_filename = Path(self.output_path).joinpath(f'{stream_label}[{ts},{ts+block_size}].json')
                with open (output_filename,'w') as fp:
                    json.dump(res_dict, fp)
                
            elif self.output_type == 'pk':
                output_filename = Path(self.output_path).joinpath(f'{stream_label}[{ts},{ts+block_size}].pk')
                with open (output_filename,'wb') as fp:
                    pickle.dump(res_dict, fp)
            else:
                pass
        
        return []

    def process(self): 
        dli = PyDli(caseDir=self.path, dliVersion=self.version)
        dli_dup =  deepcopy(dli)
        
        if self.tsRange is None:  
            dli.loadDli()
            dli.find_stream_path(self.stream_label, search_path=self.path)
            self.tsRange = list(dli.DliReader.GetFirstLastKey(str(dli.stream_path)))
            print(self.tsRange)
            
        print(self.tsRange)
        tRangeList = arange(*self.tsRange, self.block_size )
        
        # # # # Use ProcessPoolExecutor to execute functions in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures =[executor.submit(self.worker, dli_dup, self.stream_label, value, self.block_size) for value in tRangeList]
            results=[]
            for future in concurrent.futures.as_completed(futures): 
                results.extend(future.result())

            # results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        print("All tasks completed.")     
       
        return {'Num of records': len(results)}
    
    
if __name__ == "__main__":
    dli = MP_dli(version = '8.1.1.944', 
                 path=r'L:\Carto_Recording_13\Carto V8 Phase 3\TPI field investigations\274554\Recordings\2025.06.05_10.38.14.357',
                 stream_label = 'tracettalgupdate2', tsRange = {2_200_000, 2_301_000}, 
                 max_workers = 4, 
                 output_path = r'C:\TEMP\dliDumps')
    dli.process()