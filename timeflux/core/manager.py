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
        """Launch as many workers as there are graphs."""
        try:
            # Launch workers
            self._launch()
            # Monitor them
            self._monitor()
        except KeyboardInterrupt:
            # Ignore further interrupts
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            logging.info('Interrupting')
        # Terminate gracefully
        self._terminate()


    def _launch(self):
        """Launch workers."""
        self._processes = []
        for graph in self.config['graphs']:
            worker = Worker(graph)
            pid = worker.run()
            self._processes.append(psutil.Process(pid))
            logging.debug("Worker spawned with PID %d", pid)


    def _monitor(self):
        """Wait for at least one worker to terminate."""
        while True:
            for process in self._processes:
                if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
                    return
            time.sleep(.1)


    def _terminate(self):
        """Terminate all workers."""
        # https://bugs.python.org/issue26350
        sig = signal.CTRL_C_EVENT if sys.platform == 'win32' else signal.SIGINT
        # Try to terminate gracefully
        for process in self._processes:
            if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                process.send_signal(sig)
        # Kill the remaining ones
        try:
            gone, alive = psutil.wait_procs(self._processes, timeout=10)
            for process in alive:
                process.kill()
        except:
            pass


    def _load_yaml(self, filename):
        with open(filename) as stream:
            return yaml.safe_load(stream)


    def _load_json(self, filename):
        with open(filename) as stream:
            return json.load(stream)


    def _validate(self):
        path = str(pathlib.Path(__file__).parents[1].joinpath('schema', 'app.json'))
        schema = self._load_json(path)
        return ValidateWithDefaults(schema, self.config)
