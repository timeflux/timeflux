Timeflux documentation
======================

Timeflux is an open-source platform for realtime acquisition and processing of biosignals. It can easily be operated by experimenters and hackers with a unique configuration file, in which graphs and processing nodes are expressed using a simple YAML syntax. Written in Python and resting on industry standards such as Pandas, Numpy, and Scipy, it is meant to be easily extensible. Core modules include common signal processing nodes, a monitoring web interface, network communication protocols (Lab Streaming Layer, ZeroMQ, raw TCP, OSC), and HDF5 file handling (saving and replay). Drivers for open and proprietary hardware (EEG, ECG, PPG, GSR, respiration, eye tracking) have already been developed, with more coming.

.. warning::
    Use at your own risk.


.. toctree::
    :hidden:
    :caption: Table of Contents


.. raw:: latex

    \part{General}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: General
    :name: general

    general/about.rst
    general/concepts.rst


.. raw:: latex

    \part{Usage}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Usage
    :name: usage

    usage/getting_started.rst
    usage/extending.rst


.. raw:: latex

    \part{API reference}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: API reference
    :name: api
    :glob:

    api/modules_*
    misc/indices.rst
