import json
import clr
from System import UInt64, Array # type: ignore
from pathlib import Path
import re
import sys, os
import configparser
import shutil
import logging

class PyDli(): 
    def __init__(self, caseDir = None, dliVersion=None):
        self.module_dir = Path(__file__).parent
        self.caseDir = caseDir
        self.setup_logging()

        self.config = configparser.ConfigParser()
        self.config.read(self.module_dir.joinpath('config.ini'))
        if dliVersion is None: 
            self.dliVersion = self.readCartoVersion(caseDir)
        else: 
            self.dliVersion = dliVersion
        logging.info(f"set dliVersion : {self.dliVersion}")
        
        with open(self.module_dir.joinpath("stream2reader.json"), 'r') as fp: 
            self.stream2reader_ = json.load(fp)       



    def setup_logging(self):
        """Set up logging to an external file in the same directory."""
        log_file_path = os.path.join(self.module_dir, 'pyDli.log')
        logging.basicConfig(
            filename=log_file_path,
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("Logging initialized for pyDli class")

    def readCartoVersion(self, caseDir=None): 
        """ search for 'recording_version.txt' in caseDir and parse carto version  """
        if caseDir is None: 
            _caseDir = self.caseDir
        else:
            _caseDir = caseDir

        recordingsFile = Path(self.caseDir).rglob('recording_version.txt')
        for recFile in recordingsFile: 
            with open(recFile, 'r') as f: 
                for line in f: 
                    m = re.findall('WSM Version: (.*)\n',line)
                    if m: 
                        return m[0]
        return False   

    def updateDLiPath(self, doCopy=True): 

        assert self.dliVersion ,'No CARTO version was set !'
        self.base_local_dli_path = Path(self.config['DEFAULT']['base_local_dli_path'])
        self.local_dli_path = self.base_local_dli_path.joinpath( self.dliVersion)
        self.remote_dli_path = Path(self.config['DEFAULT']['remote_dli_path']).joinpath( self.dliVersion)

        if doCopy: 
            # copy DLL's to local dst
            if not(os.path.exists(self.local_dli_path)):
                # print('copy dli from remote')
                logging.info('copy dli files')
                shutil.copytree(self.remote_dli_path, self.local_dli_path)
                logging.info('dli files were copied')

    def loadDli(self): 
        # copy dli directory locally
        self.updateDLiPath()
         # load assambly
        sys.path.append(str(self.local_dli_path))
        try:
            clr.FindAssembly("DliNetInterface")
            clr.AddReference("DliNetInterface")
            logging.info(f'load DLI V{self.dliVersion}')
        except: 
            logging.error(f'unable to load dLI : {self.local_dli_path}')
            
        from DliNetInterface import DliReader # type: ignore
        self.DliReader = DliReader
        return DliReader

    def stream2reader(self, stream_label): 
        read_func = self.stream2reader_[stream_label]['reader']
        logging.info(f'use {read_func} to read {stream_label}')
        return getattr(self.DliReader, read_func, None)
    
    def find_stream_path(self, stream_label, search_path=None): 
        logging.info('find_stream_path: search for stream_path: {stream_label}')
        # check if stream exists 
        if not search_path: 
            search_path = self.caseDir

        stream_flist = list(Path(search_path).rglob(stream_label + '*.1'))
        num_of_streams = len(stream_flist)
        logging.info('find_stream_path: find : {num_of_streams} streams of {stream_label}')
        if not stream_flist: 
            return False
        
        # assert len(stream_flist) == 1 , "too many streams were find"
        if len(stream_flist) > 1: 
            logging.warning( "too many streams were find, use the 1st one")
            
        self.stream_path = str(stream_flist[0].with_suffix(''))
        return str(stream_flist[0].with_suffix(''))
    
    def get_first_last_key(self, stream_label):
        return list(self.DliReader.GetFirstLastKey(self.find_stream_path(stream_label)))
        



    def read(self, stream_label=None, ts_range=None): 
        '''
        Contract a DLI reading function. if not exact define search for "similar" function. 
        e.g. catheterpositions1 -> catheterpositions
        '''
        reading_func = self.stream2reader(stream_label)
        if not self.stream_path : 
            self.stream_path =self.find_stream_path(stream_label)


        if ts_range: 
            return reading_func(self.stream_path, UInt64(ts_range[0]), UInt64(ts_range[1]))
        else: 
            return reading_func(self.stream_path)
        
    def parse(self, trace=None, stream_label=None, **kwarg): 
        '''
        Contract a parsing function. if not define in json file, use read_generic function. 
        '''
        if trace and stream_label: 
            # parser_ = self.stream2reader_[stream_label]['parser']
            # if parser_ == '' : 
            #     parser_ = 'read_generic'
            # parser_ = 'read_generic'
            # constract_parser = f'parser.{parser_}'

            # parser_func = eval(constract_parser)
            parser_func = self.read_generic
            return  [parser_func(x, **kwarg) for x in trace]
            
        else: 
            print('Missing trace and/or stream_label')
            return []

    def read_generic(self, rec): 
        out = dict()
        for elem_name in dir(rec): 
            elem = getattr(rec, elem_name, None)
            # if not DLI class 
            if elem_name.startswith('__') or (str(type(elem)) == "<class 'CLR.MethodBinding'>"): 
                continue
            if isinstance(elem, Array): 
                lst = []
                for l in elem : 
                    if 'DliNetInterface' in str(type(l)): 
                        lst.append(self.read_generic(l))
                    else: 
                        lst.append(l)
                out[elem_name] = lst
            elif 'DliNetInterface' in str(type(elem)): 
                out[elem_name] =self.read_generic(elem)
            else:  
                out[elem_name] = elem    

        return out    
   




    def __repr__(self):
        repr_dict = {a: str(getattr(self,a)) for a in dir(self) if not a.startswith('__') and not callable(getattr(self, a))}
        return json.dumps(repr_dict, indent = 4) 

if __name__=="__main__": 

    case_dir = r"L:\Carto_Recording_13\Carto V8 Phase 3\TPI field investigations\274554\Recordings\2025.06.05_10.38.14.357"
    dli = PyDli(case_dir)
    dli.loadDli()
    print(dli)
    print(dli.get_first_last_key(stream_label='tracettalgupdate2'))
    trace=dli.read('tracettalgupdate2', ts_range=[1_000_000, 1_100_100])
    print(dli.parse(trace, stream_label='tracettalgupdate2'))
