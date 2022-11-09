import json
import os

path = os.path.abspath('dir/executor_output.json')

os.makedirs(os.path.dirname(path), exist_ok=True)

with open(path, 'w') as f:
    f.write(json.dumps({'k': 'v'}))
