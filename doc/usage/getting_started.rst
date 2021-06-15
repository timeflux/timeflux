.. _getting_started:

Getting started
===============

Installation
------------

Before we can do anything, we need a Python 3.7+ distribution. We recommend `Miniconda <https://docs.conda.io/en/latest/miniconda.html>`__. If you don't already have it, go ahead and install it.

If you intend to install from source, you will also need `Git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`__.

Now that the prerequisites are satisfied, the next order of business is to install Timeflux and its dependencies. To keep things nice and clean, we will do this in a new virtual environment. Depending on your system, open a shell or command prompt:

.. code-block:: bash

    conda create --name timeflux python=3.9 pytables bottleneck
    conda activate timeflux
    pip install timeflux
    pip install timeflux_example

If everything went well, Timeflux is now installed. Hooray!


Basic usage
-----------

Applications are self-described in YAML files. Running an app is easy:

.. code-block:: bash

    timeflux app.yaml


Let's try!
----------

First, download a very simple app that we will use as an example:

.. code-block:: bash

    curl -O https://raw.githubusercontent.com/timeflux/timeflux/master/examples/test.yaml

If the `timeflux` environment is not already activated, do it:

.. code-block:: bash

    conda activate timeflux

You can now run the test app:

.. code-block:: bash

    timeflux -d test.yaml

You should see and a bunch of random numbers every second. Hit `Ctrl+C` to stop.

Did you notice the ``-d`` flag in the command line? It's a shorthand for ``--debug``, and this what allowed us to see the messages in the console.


Command line options
--------------------

There are only a few options, and you can list them with:

.. code-block:: bash

    timeflux --help

This should print:

.. code-block:: none

    usage: timeflux [-h] [-v] [-d] [-E ENV_FILE] [-e ENV] app

    positional arguments:
      app                   path to the YAML or JSON application file

    optional arguments:
      -h, --help            show this help message and exit
      -v, --version         show program's version number and exit
      -d, --debug           enable debug messages
      -E ENV_FILE, --env-file ENV_FILE
                            path to an environment file
      -e ENV, --env ENV     environment variables

Besides the ``-d`` flag we already discussed, two options are worth mentioning: ``-E`` or ``--env-file`` and ``-e`` or ``--env``.


Environment
-----------

Storing an app configuration in the environment is a `good practice <https://12factor.net/config>`_. There are a few ways of doing this:

If a file named `.env` is found in the current directory or in any of its parent directories, it will be loaded. A `.env` file looks like this:

.. code-block:: bash

    # A comment that will be ignored
    FOO=bar
    MEANING_OF_LIFE=42


As we saw earlier, you can also specify a custom path to an environment file with the ``--env-file`` option.

Another way of setting environment variables is with the ``-e`` option:

.. code-block:: bash

    timeflux -e FOO="bar" -e MEANING_OF_LIFE=42 app.yaml

Finally, you can temporarily set environment variables for the duration of the session, directly from the console.

Windows:

.. code-block:: bash

    set FOO "bar"

Linux, MacOS:

.. code-block:: bash

    export FOO="bar"

The following environment variables are understood by Timeflux:

- ``TIMEFLUX_LOG_LEVEL_CONSOLE`` -- This is the level of details printed in the console. Possible values are `DEBUG`, `INFO`, `WARNING`, `ERROR` and `CRITICAL`. The default value is `INFO`. Running the ``timeflux`` command with the ``-d`` flag is the same as setting this variable to `DEBUG`.
- ``TIMEFLUX_LOG_LEVEL_FILE`` -- This is the logging level when the output of the application is written to a file. This variable accepts the same values as previously. The default value is ``DEBUG``. Standard `format codes <https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes>`_ are accepted.
- ``TIMEFLUX_LOG_FILE`` -- If set to a valid path, Timeflux will write the application output to a log file.
- ``TIMEFLUX_SLEEP`` -- When a graph has a rate of zero, it will run as fast as possible, but will result in a high CPU load. Setting this variable to a non-zero value can help mitigating this issue. Default is `0`.
- ``TIMEFLUX_HOOK_PRE`` -- Name of a Python module that will be run before executing the app.
- ``TIMEFLUX_HOOK_POST`` -- Name of a Python module that will be run after executing the app.

Others variables may be used by specific nodes and plugins. Refer to the relevant documentation for details.

By combining environment variables and :ref:`templating <templates>`, you can add logic to your `YAML` files and build configurable applications.


Plugins
-------

Timeflux is modular. The ``timeflux`` Python package contains the core features and the most essential nodes. Plugins are standard Python packages that provide one or several nodes. Officially supported plugins can be found on `Timeflux GitHub page <https://github.com/timeflux>`_. Some plugins (especially those dealing with hardware) have special requirements. Please refer to each plugin repository for installation instructions.

Notable plugins include:

    * `User interface <https://github.com/timeflux/timeflux_ui>`_
    * `Digital Signal Processing <https://github.com/timeflux/timeflux_dsp>`_
