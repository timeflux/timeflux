"""timeflux.core.manager: manage workers"""

import logging
import sys
import time
import signal
import psutil
import pathlib
import json
import yaml
from jsonschema.exceptions import ValidationError
from timeflux.core.validate import ValidateWithDefaults
from timeflux.core.worker import Worker


class Manager:

    """Load configuration and spawn workers."""

    def __init__(self, config):
        """
        Load configuration

        Parameters
        ----------
        config : str|dict
            The configuration can either be a path to a YAML or JSON file, a JSON string or a dict.

        """

        # Load config
        if isinstance(config, dict):
            self.config = config
        elif isinstance(config, str):
            extension = config.split('.')[-1]
            if extension in ('yml', 'yaml'):
                self.config = self._load_yaml(config)
            elif extension == 'json':
                self.config = self._load_json(config)
            else:
                self.config = json.loads(config)
        else:
            raise ValueError('Could not load application file')

        # Validate
        if not self._validate():
            raise ValueError('Invalid application file')


    def run(self):
        """Span as many workers as there are graphs."""

        processes = []

        # Launch workers
        for graph in self.config['graphs']:
            worker = Worker(graph)
            pid = worker.run()
            processes.append(psutil.Process(pid))
            logging.debug("Worker spawned with PID %d", pid)

        # Wait for workers to terminate
        self._loop(processes)
        self._terminate(processes)


    def _loop(self, processes):
        while True:
            for process in processes:
                if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
                    return
            time.sleep(.1)


    def _terminate(self, processes):
        # https://bugs.python.org/issue26350
        sig = signal.CTRL_C_EVENT if sys.platform == 'win32' else signal.SIGINT
        # Try to terminate gracefully
        for process in processes:
            if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                process.send_signal(sig)
        # Kill the remaining ones
        gone, alive = psutil.wait_procs(processes, timeout=5)
        for process in alive:
            process.kill()


    def _load_yaml(self, filename):
        with open(filename) as stream:
            return yaml.load(stream)


    def _load_json(self, filename):
        with open(filename) as stream:
            return json.load(stream)


    def _validate(self):
        path = str(pathlib.Path(__file__).parents[1].joinpath('schema', 'app.json'))
        schema = self._load_json(path)
        return ValidateWithDefaults(schema, self.config)
