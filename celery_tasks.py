from celery import Celery
import os, sys
import time
# from pyDli.mp_dli import MP_dli  # Importing MP_dli for DLI processing
from pyDli.pyDli import PyDli
from pht_runner.pht_runner import PHT_runner  # Importing PHT_runner for PHT processing
from pathlib import Path
import pickle, json


# Celery configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://wdcoretech02:6379/0")  # Default Redis URL for Celery broker and backend


app = Celery(
    "case_processor",  # Name of the Celery application
    broker=REDIS_URL,  # Broker URL for task queue
    backend=REDIS_URL  # Backend URL for storing task results
)

app.conf.update(
    task_serializer="json",  # Serialize tasks as JSON
    result_serializer="json",  # Serialize results as JSON
    accept_content=["json"],  # Accept only JSON content
    timezone="UTC",  # Set timezone to UTC
    enable_utc=True,  # Enable UTC for task scheduling
)


@app.task(name='tasks.add')
def add(x, y):
    """
    A simple addition task.
    Args:
        x (int): First number.
        y (int): Second number.
    Returns:
        int: Sum of x and y.
    """
    return x + y

@app.task(name='tasks.genric_task')
def generic_task(type: str, params: dict):
    """
    A generic task that performs actions based on the type.
    Args:
        type (str): Type of task ('echo' or others).
        params (dict): Parameters for the task.
    Returns:
        dict: Parameters passed to the task.
    """
    if type == 'echo':
        time.sleep(5)  # Simulate a delay
        print(params)  # Print the parameters
    else:
        print('unknown type!!!!')  # Handle unknown types
    return params


@app.task(name='tasks.dli_read', bind=True)
def dli_read(self, params: dict):
    self.update_state(state='PROGRESS', meta={'step': 'Initializing DLI'})
    """
    A task to read data using the MP_dli module.
    Args:
        params (dict): Parameters for the DLI process.
            Required keys: 'version', 'path', 'stream_label', 'output_path'.
            Optional keys: 'tsRange', 'max_workers'.
    Returns:
        bool or str: True if successful, error message otherwise.
    """
    print(params)
    required_keys = ['version', 'path', 'stream_label', 'output_path']
    if all(k in params for k in required_keys):
        
        stream_label = params.get('stream_label')
        output_path = params.get('output_path')
        output_type = params.get('output_type', 'json')
        tsRange=params.get('tsRange', None)

        if not Path(output_path).exists(): 
            raise 'output_path is not exist'
        
        try:
            with open(Path(output_path).joinpath('test.txt'), 'w') as f: 
                f.write('.')
        except: 
            raise 'output_path is not exaccessableist'

        dli = PyDli(caseDir=params.get('path'), 
                    dliVersion = params.get('version', None))
        self.update_state(state='PROGRESS', meta={'step': 'Loading DLI'})
        dli.loadDli()
        print(dli.get_first_last_key(stream_label=stream_label))
        trace=dli.read(stream_label, ts_range=params.get('tsRange', None))
        self.update_state(state='PROGRESS', meta={'step': 'Reading Data'})
        res_dict = dli.parse(trace, stream_label=stream_label)
        self.update_state(state='PROGRESS', meta={'step': 'Parsing Data'})
        
        if output_path: 
            self.update_state(state='PROGRESS', meta={'step': 'Saving Output'})
            if output_type == 'json':
                output_filename = Path(output_path).joinpath(f'{stream_label}[{tsRange}].json')
                if output_type == 'json':
                    with open(output_filename, 'w') as fp:
                        json.dump(res_dict, fp)
                elif output_type == 'pk':
                    with open(output_filename, 'wb') as fp:
                        pickle.dump(res_dict, fp)

        

        return True
    else:
        return 'Missing params in pyDli process'

@app.task(name='tasks.pht_run', bind=True)
def pht_run(self, params: dict):
    self.update_state(state='PROGRESS', meta={'step': 'Initializing PHT process'})
    """
    A task to run the PHT process using the PHT_runner module.
    Args:
        params (dict): Parameters for the PHT process.
            Required keys: 'path'.
    Returns:
        bool: True if successful.
    """

    pht = PHT_runner(dataDir= params.get('dataDir'), 
                     label=params.get('label'), 
                     enable_traces=params.get('enable_traces'), 
                     exclude_dirs=params.get('enable_traces', None) )
    print(pht)
    self.update_state(state='PROGRESS', meta={'step': 'Reading Carto version'})
    pht.readCartoVersion()
    self.update_state(state='PROGRESS', meta={'step': 'Copying PHT tester files'})
    pht.copy_phtester()
    self.update_state(state='PROGRESS', meta={'step': 'Copying recordings'})
    pht.copy_recordings()
    self.update_state(state='PROGRESS', meta={'step': 'Updating trace configurations'})
    pht.update_trace_config()
    self.update_state(state='PROGRESS', meta={'step': 'Running PHT tester'})
    pht.run_phtester()

    return True














if __name__ == '__main__':
    args = ['worker', '--pool=threads', '--loglevel=INFO', '--concurrency=2']
    app.worker_main(argv=args)  # Start the Celery worker with specified arguments

#  --pool=gevent --concurrency=8
#  --pool=solo
#  --pool=threads --concurrency=8
# uv run celery -A celery_tasks.app flower --port=5555 --basic-auth="liron:123"
