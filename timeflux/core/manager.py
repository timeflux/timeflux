"""timeflux.core.manager: manage workers"""

import logging
import sys
import os
import time
import signal
import json
import yaml
from timeflux.core.validate import validate
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

        # Initialize logger
        self.logger = logging.getLogger(__name__)

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

        # Hold the children processes
        self._processes = []


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
            self.logger.info('Interrupting')
        # Terminate gracefully
        self._terminate()


    def _launch(self):
        """Launch workers."""
        for graph in self.config['graphs']:
            worker = Worker(graph)
            process = worker.run()
            self._processes.append(process)
            self.logger.debug("Worker spawned with PID %d", process.pid)


    def _monitor(self):
        """Wait for at least one worker to terminate."""
        while True:
            if any(not process.is_alive() for process in self._processes):
                return
            time.sleep(.1)


    def _terminate(self):
        """Terminate all workers."""
        # https://bugs.python.org/issue26350
        interrupt = signal.CTRL_C_EVENT if sys.platform == 'win32' else signal.SIGINT
        # Try to terminate gracefully
        for process in self._processes:
            if process.is_alive():
                os.kill(process.pid, interrupt)
        # Wait 10 seconds and kill the remaining ones
        self._wait(10)
        for process in self._processes:
            if process.is_alive():
                process.terminate()


    def _wait(self, timeout=None):
        """Wait for all workers to die."""
        if not self._processes: return
        start = time.time()
        while True:
            try:
                if all(not process.is_alive() for process in self._processes):
                    # All the workers are dead
                    return
                if timeout and time.time() - start >= timeout:
                    # Timeout
                    return
                time.sleep(.1)
            except:
                pass


    def _load_yaml(self, filename):
        with open(filename) as stream:
            return yaml.safe_load(stream)


    def _load_json(self, filename):
        with open(filename) as stream:
            return json.load(stream)


    def _validate(self):
        return validate(self.config)
