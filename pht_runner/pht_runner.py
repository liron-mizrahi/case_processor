from pathlib import Path
import re
import json
import subprocess as sp
import os, sys
import shutil
from datetime import datetime
import configparser
import logging
import xml.etree.ElementTree as ET

# This class handles the operations related to the PositionHandlerTester (PHT) tool.
class PHT_runner(): 
            

    # Initializes the PHT_runner class with the required parameters and sets up logging.
    def __init__(self, dataDir:str, label:str = "case1",enable_traces=[]) -> None:
        self.module_dir = Path(__file__).parent
        self.config = configparser.ConfigParser()
        self.config.read(self.module_dir.joinpath('config.ini'))
        self.dataDir = dataDir
        self.label = label
        self.enable_traces = enable_traces

        """Set up logging to an external file in the same directory."""
        log_file_path = os.path.join(self.module_dir, 'pht_runner.log')
        logging.basicConfig(
            filename=log_file_path,
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("Logging initialized for pht_runner class")

        


    # Searches for the CARTO version in the recordings directory and sets caseDir and cartoVersion.
    def readCartoVersion(self): 
        logging.info('searching for CARTO version...')
        recordingsFile = Path(self.dataDir).rglob('**/recording_version.txt')
        for recFile in recordingsFile: 
            logging.info(f'found recording_version in : {recFile}')
            with open(recFile, 'r') as f: 
                for line in f: 
                    m = re.findall('WSM Version: (.*)\n',line)
                    if m: 
                        self.caseDir = recFile.parent
                        self.cartoVersion = m[0]
                        logging.info(f'caseDir: {self.caseDir}')
                        logging.info(f'cartoVersion: {self.cartoVersion}')
                        return True
        return False   
    
    # Copies the corresponding PHTester version from the remote repository to the local directory.
    def copy_phtester(self):

        self.local_pht_path = Path(self.config['DEFAULT']['local_phtester_path']).joinpath(self.cartoVersion)
        self.remote_pht_path = Path(self.config['DEFAULT']['phtester_repository_path']).joinpath(self.cartoVersion)
        logging.info(f"search for corecponding phtester version in {self.remote_pht_path}")
        
        if not self.remote_pht_path.exists() : 
            logging.error(f'unable to find... ({self.remote_pht_path})')
        if not self.local_pht_path.exists() : 
            try: 
                logging.info(f"copy phtester from remote repository to local ")
                shutil.copytree(self.remote_pht_path, self.local_pht_path  )
            except FileExistsError: 
                logging.info(f'local phtester already exists! ({self.cartoVersion})')
        else: 
            logging.info(f'local phtester already exists! ({self.cartoVersion})')
    
    # Copies recordings from the case directory to the incoming temporary directory.
    def copy_recordings(self):
        self.incoming_case_path = Path(self.config['DEFAULT']['local_phtester_incoming_temp']).joinpath(self.label)
        
        logging.info('check if case label exist in incoming temp dir...')
        if self.incoming_case_path.exists(): 
            logging.info(f'case : {self.incoming_case_path} already exist, skip copying...')
            return True 

        logging.info(f'create new directory : {self.incoming_case_path}')
        os.makedirs(self.incoming_case_path)
        

        for f in Path(self.caseDir).iterdir(): 
            if f.is_dir(): 
                if f.parts[-1] not in ['CartoData' , 'ScreenRecording', 'Traces']: 
                    print(f)
                    shutil.copytree(f, Path(self.incoming_case_path).joinpath(f.name), ignore=shutil.ignore_patterns('*$RECYCLE.BIN*'))
            else: 
                print(f)
                shutil.copy(f, self.incoming_case_path)

    # Updates the trace configuration XML file to enable or disable specific traces.
    def update_trace_config(self):
        
        self.enable_traces.append('TraceVersion')

        xml_filename = Path(self.incoming_case_path).joinpath('Configuration/TracerConfig.xml')
        if not xml_filename.exists(): 
            logging.warning('unable to find TracerConfig.xml')
        
        
        tree = ET.parse(xml_filename)
        root = tree.getroot()
        
        # Modify elements
        for elem in root.findall('Trace'):
            if elem.attrib['Name'] in self.enable_traces: 
                elem.attrib['Enabled'] = "true"
                logging.info(f'enable trace : {elem.attrib['Name']}')
            else: 
                elem.attrib['Enabled'] = "false"

        tree.write(xml_filename)
        logging.info('rewrite xml : {xml_filename}')
        return True
    
    # Executes the PHTester tool using the provided batch file and paths.
    def run_phtester(self): 
        self.pht_local_output_path = Path(self.config['DEFAULT']['local_phtester_output_temp']).joinpath(self.label)
        cmdStr=r'"{0}" "{1}" "{2}"'.format(str(self.local_pht_path.joinpath('PositionHandlerTester.bat')), 
                                   self.incoming_case_path, 
                                   self.pht_local_output_path)

        logging.info(f'Run : {cmdStr}')
        # result = sp.run(cmdStr, stdout=sp.PIPE)
        # return result.stdout.strip().decode("utf-8") 
    
    # Moves the generated trace results from the local output directory to the remote output directory.
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
            
    # Cleans up temporary directories created during the PHTester run.
    def cleanup(self): 
        shutil.rmtree(self.pht_local_output_path, ignore_errors=True)
        shutil.rmtree(self.incoming_case_path)


    # Executes the full workflow of the PHT_runner class, including reading the CARTO version, copying files, updating configurations, and running the PHTester tool.
    def run(self): 
        self.readCartoVersion()
        self.copy_phtester()
        self.copy_recordings()
        self.update_trace_config()
        self.run_phtester()
        # self.copy_result()
        # self.cleanup()

    # Returns a JSON representation of the PHT_runner class attributes for debugging purposes.
    def __repr__(self):
        repr_dict = {a: str(getattr(self,a)) for a in dir(self) if not a.startswith('__') and not callable(getattr(self, a))}
        return json.dumps(repr_dict, indent = 4) 



# Entry point for running the PHT_runner class with sample parameters.
if __name__=="__main__": 
    pht = PHT_runner(dataDir= r'L:\Carto_Recording_13\Carto V8 Phase 3\TPI field investigations\274554\Recordings\2025.06.05_10.38.14.357', 
                     label="case01", 
                     enable_traces=['TraceTTAlgUpdate2', 'TraceBCSystem'])
    print(pht)
    print(pht.run())
