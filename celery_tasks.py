from celery import Celery
import os, sys
import time
from pyDli.mp_dli import MP_dli  # Importing MP_dli for DLI processing
from pht_runner.pht_runner import PHT_runner  # Importing PHT_runner for PHT processing

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


@app.task(name='tasks.dli_read')
def dli_read(params: dict):
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
        dli = MP_dli(
            version=params.get('version'),
            path=params.get('path'),
            stream_label=params.get('stream_label'),
            tsRange=params.get('tsRange', None),
            max_workers=params.get('max_workers', 1),
            output_path=params.get('output_path')
        )
        dli.process()  # Process the data
        return True
    else:
        return 'Missing params in pyDli process'

@app.task(name='tasks.pht_run')
def pht_run(params: dict):
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
                     enable_traces=params.get('enable_traces'))
    print(pht)
    pht.run()

    return True














if __name__ == '__main__':
    args = ['worker', '--pool=threads', '--loglevel=INFO', '--concurrency=2']
    app.worker_main(argv=args)  # Start the Celery worker with specified arguments

#  --pool=gevent --concurrency=8
#  --pool=solo
#  --pool=threads --concurrency=8
# uv run celery -A celery_tasks.app flower --port=5555 --basic-auth="liron:123"
