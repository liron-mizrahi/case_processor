from pathlib import Path
import re
import json
import subprocess as sp
import os, sys
import shutil
from datetime import datetime
import configparser



class PHT_runner(): 

    def __init__(self, dataDir=None) -> None:
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.pht_remote_output_path = ''
        self.caseDir = ''

        if dataDir: 
            self.prefix = datetime.now().strftime("%d.%m.%Y.%H.%M.%S") # + "  " + "-".join(Path(dataDir).parts[-3:])
            self.dataDir = dataDir
            if Path(self.dataDir).joinpath('source.txt').exists(): 
                with open(Path(self.dataDir).joinpath('source.txt'), 'rt') as f: 
                    self.caseDir = f.readline()
            




    def __repr__(self) -> str:
        pass

    def readCartoVersion(self,caseDir): 
        recordingsFile = os.path.join(caseDir, 'recording_version.txt')

        with open(recordingsFile, 'r') as f: 
            for line in f: 
                m = re.findall('WSM Version: (.*)\n',line)
                if m: 
                    return m[0]
        return False
    
    def set_carto_ver(self, carto_ver=None, caseDir=None): 
        if carto_ver: 
            self.carto_ver = carto_ver
        elif caseDir: 
            self.caseDir = caseDir
            self.carto_ver = self.readCartoVersion(caseDir)

    def copy_phtester(self, caseDir=None):
        # if caseDir: 
        #     self.caseDir = caseDir
        #     self.carto_ver = self.readCartoVersion(caseDir)
        # else: 
        #     self.carto_ver = self.readCartoVersion(self.caseDir)

        self.local_pht_path = Path(self.config['DEFAULT']['local_phtester_path']).joinpath(self.carto_ver)
        self.remote_pht_path = Path(self.config['DEFAULT']['phtester_repository_path']).joinpath(self.carto_ver)
        
        if self.remote_pht_path.exists() : 
            try: 
                shutil.copytree(self.remote_pht_path, self.local_pht_path,  )
            except FileExistsError: 
                print('local phtester already exists!')
                pass 

    
    def copy_recordings(self):

        self.incoming_case_path = Path(self.config['DEFAULT']['local_phtester_incoming_temp']).joinpath(self.prefix)
        if not os.path.exists(self.incoming_case_path): 
            os.makedirs(self.incoming_case_path)
            print(f'Create new directory: {self.incoming_case_path}')

        for f in Path(self.caseDir).iterdir(): 
            if f.is_dir(): 
                if f.parts[-1] not in ['CartoData' , 'ScreenRecording', 'Traces']: 
                    print(f)
                    shutil.copytree(f, Path(self.incoming_case_path).joinpath(f.name), ignore=shutil.ignore_patterns('*$RECYCLE.BIN*'))
            else: 
                print(f)
                shutil.copy(f, self.incoming_case_path)

    def update_trace_config(self):
        shutil.copy(Path(self.config['DEFAULT']['local_phtester_config_path']).joinpath('TracerConfig.xml'), 
            Path(self.incoming_case_path).joinpath('Configuration/TracerConfig.xml'))
        
    def update_TT_config(self):
        shutil.copy(Path(self.config['DEFAULT']['local_phtester_config_path']).joinpath('TTCoreConfig.xml'), 
            Path(self.incoming_case_path).joinpath('Configuration/TTCoreConfig.xml'))
                
    def update_registry_snapshot(self):
        shutil.copy(Path(self.config['DEFAULT']['local_phtester_config_path']).joinpath('registry_snapshot_ascii.reg'), 
            Path(self.incoming_case_path).joinpath('registry_snapshot_ascii.reg'))

    def update_catheter_xml(self, cath_prefix = 'Connector#2_1423'): 
        for f in Path(self.caseDir).glob(cath_prefix+'*_model.xml'): 
            shutil.copy(f, f.with_suffix('.xml.bak')) # rename orig xml file
            xml_template = Path(self.config['DEFAULT']['local_phtester_config_path']).joinpath(cath_prefix+'_template_model.xml')
            shutil.copy(xml_template,  Path(self.caseDir).joinpath(f.parts[-1]))

    def run_phtester(self): 
        self.pht_local_output_path = Path(self.config['DEFAULT']['local_phtester_output_temp']).joinpath(self.prefix)
        cmdStr=r'"{0}" "{1}" "{2}"'.format(str(self.local_pht_path.joinpath('PositionHandlerTester.bat')), 
                                   self.incoming_case_path, 
                                   self.pht_local_output_path)

        print(cmdStr)
        result = sp.run(cmdStr, stdout=sp.PIPE)
        return result.stdout.strip().decode("utf-8") 
    
    def copy_result(self): 
        if self.pht_remote_output_path == '' :
            self.pht_remote_output_path = Path(self.dataDir).joinpath('Traces')
        try: 
            trace_flist = list(self.pht_local_output_path.rglob('tracettalgupdate2.1'))
            print(trace_flist)
            for trace in trace_flist: 
                print(trace)
                shutil.move(trace.parent, Path(self.pht_remote_output_path).joinpath(trace.parts[-3]))
        except FileNotFoundError as e: 
            print(e)
            



    def cleanup(self): 
        shutil.rmtree(self.pht_local_output_path, ignore_errors=True)
        shutil.rmtree(self.incoming_case_path)


    def run(self): 
        self.copy_phtester()
        self.copy_recordings()
        self.update_trace_config()
        # self.update_catheter_xml()
        self.run_phtester()
        self.copy_result()
        self.cleanup()

    def __repr__(self):
        repr_dict = {a: str(getattr(self,a)) for a in dir(self) if not a.startswith('__') and not callable(getattr(self, a))}
        return json.dumps(repr_dict, indent = 4) 



if __name__=="__main__": 
    pht = PHT_runner(r'L:\Carto_Recording_13\Carto V8 Phase 3\TPI field investigations\274554\Recordings\2025.06.05_10.38.14.357')
    print(pht)





