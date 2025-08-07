# Case Processor

This project provides a Celery-based task processing system for various operations, including addition, generic tasks, DLI reading, and PHT running.

## Requirements

- Python 3.x
- Redis server

## Setup

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set the `REDIS_URL` environment variable if you want to use a custom Redis server:
   ```bash
   export REDIS_URL=redis://your_redis_server:6379/0
   ```

3. Start the Celery worker:
   ```bash
   python celery_tasks.py
   ```

## Tasks

### 1. Addition Task
Adds two numbers.
- **Task Name**: `tasks.add`
- **Example**:
  ```python
  result = app.send_task('tasks.add', args=[5, 3])
  print(result.get())  # Output: 8
  ```

### 2. Generic Task
Performs a generic operation based on the type.
- **Task Name**: `tasks.genric_task`
- **Example**:
  ```python
  result = app.send_task('tasks.genric_task', kwargs={'type': 'echo', 'params': {'message': 'Hello, World!'}})
  print(result.get())  # Output: {'message': 'Hello, World!'}
  ```

### 3. DLI Read Task
Processes data using the DLI module.
- **Task Name**: `tasks.dli_read`
- **Example**:
  ```python
  params = {
      'version': '1.0',
      'path': '/data/input',
      'stream_label': 'stream1',
      'output_path': '/data/output'
  }
  result = app.send_task('tasks.dli_read', kwargs={'params': params})
  print(result.get())  # Output: True
  ```

### 4. PHT Run Task
Runs the PHT process.
- **Task Name**: `tasks.pht_run`
- **Example**:
  ```python
  params = {'path': '/cases/case1'}
  result = app.send_task('tasks.pht_run', kwargs={'params': params})
  print(result.get())  # Output: True
  ```

## Flower API Interface

Flower is a web-based tool for monitoring and managing Celery clusters.

### Starting Flower
Run the following command to start Flower:
```bash
uv run celery -A celery_tasks.app flower --port=5555 --basic-auth="username:password"
```

### Adding Tasks via Flower API
1. Open Flower in your browser at `http://localhost:5555`.
2. Authenticate using the credentials provided in the `--basic-auth` flag.
3. Navigate to the "Tasks" section.
4. Use the "Send Task" form to add tasks. Provide the task name and arguments as required.

### Example: Adding a Task
To add a `tasks.add` task:
1. Task Name: `tasks.add`
2. Arguments: `[5, 3]`
3. Click "Send Task".

To add a `tasks.dli_read` task:
1. Task Name: `tasks.dli_read`
2. Arguments: `{"params": {"version": "1.0", "path": "/data/input", "stream_label": "stream1", "output_path": "/data/output"}}`
3. Click "Send Task".

## License

This project is licensed under the MIT License.
