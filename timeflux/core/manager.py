"""timeflux.core.manager: manage workers"""

import logging
import sys
import os
import time
import signal
import json
import yaml
from jinja2 import Template
from timeflux.core.validate import validate
from timeflux.core.worker import Worker


class Manager:

    """Load configuration and spawn workers."""

    def __init__(self, config):
        """Load configuration

        Args:
            config (string|dict): The configuration can either be a path to a YAML or JSON file or a dict.

        """

        # Initialize logger
        self.logger = logging.getLogger(__name__)

        # Hold the names of the imported applications
        self._imports = []

        # Hold the graphs
        self._graphs = []

        # Hold the children processes
        self._processes = []

        # Load application
        if isinstance(config, dict):
            app = config
            path = os.getcwd()
        elif isinstance(config, str):
            app = self._load_file(config)
            self._imports.append(os.path.abspath(config))
            path = os.path.dirname(self._imports[0])
        else:
            raise ValueError("Could not load application file")

        # Validate
        validate(app)

        # Populate the graph list
        if "graphs" in app:
            self._graphs = app["graphs"]

        # Import sub applications
        self._import(app, path)

        # Update the search path
        for app in self._imports:
            path = os.path.dirname(app)
            if path not in sys.path:
                sys.path.append(path)

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
            self.logger.info("Interrupting")
        # Terminate gracefully
        self._terminate()

    def _launch(self):
        """Launch workers."""
        for graph in self._graphs:
            worker = Worker(graph)
            process = worker.run()
            self._processes.append(process)
            self.logger.debug("Worker spawned with PID %d", process.pid)

    def _monitor(self):
        """Wait for at least one worker to terminate."""
        if not self._processes:
            return
        while True:
            if any(not process.is_alive() for process in self._processes):
                return
            time.sleep(0.1)

    def _terminate(self):
        """Terminate all workers."""
        # https://bugs.python.org/issue26350
        interrupt = signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
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
        if not self._processes:
            return
        start = time.time()
        while True:
            try:
                if all(not process.is_alive() for process in self._processes):
                    # All the workers are dead
                    return
                if timeout and time.time() - start >= timeout:
                    # Timeout
                    return
                time.sleep(0.1)
            except:
                pass

    def _import(self, app, path):
        if not "import" in app:
            return
        old_path = os.getcwd()
        os.chdir(path)
        for filename in app["import"]:
            filename = os.path.abspath(filename)
            if filename in self._imports:
                self.logger.debug("Application %s will not be loaded twice", filename)
                continue
            self.logger.debug("Importing %s", filename)
            self._imports.append(filename)
            sub = self._load_file(filename)
            try:
                validate(sub)
            except ValueError as error:
                raise ValueError(f"Validation failed ({filename})")
            if "graphs" in sub:
                self._graphs += sub["graphs"]
            self._import(sub, os.path.dirname(filename))
        os.chdir(old_path)

    def _load_file(self, filename):

        # Read file as string
        with open(filename) as stream:
            app = stream.read()

        # Parse the template
        app = self._parse_template(app)

        # Return a dict
        extension = filename.split(".")[-1]
        if extension in ("yml", "yaml"):
            return yaml.safe_load(app)
        elif extension == "json":
            return json.loads(app)

    def _parse_template(self, template):
        template = Template(template)
        return template.render(dict(os.environ))
