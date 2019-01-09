Timeflux documentation
======================

Timeflux is a free and open-source solution for the acquisition and real-time processing of biosignals.
Use it to bootstrap your research, build brain-computer interfaces, closed-loop biofeedback applications, interactive installations, and more. Written in Python, it works on Linux, MacOS and Windows. Made for researchers and hackers alike.

It comes with integrated communication protocols (Lab Streaming Layer, ZeroMQ, OSC), HDF5 file handling (saving and replay) and generic data manipulation tools.

Currently available plugins include signal processing nodes, machine learning tools and a monitoring web interface.

Drivers for open and proprietary hardware (EEG, ECG, PPG, EDA, respiration, eye tracking) have already been developed, with more coming.

.. note::
    Right now, the documentation is quite coarse, and some parts of the code need polishing. We're working on it!

.. warning::
    Timeflux is an early stage project and is actively developed. Use at your own risk.


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
    general/getting_help.rst


.. raw:: latex

    \part{Usage}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Usage
    :name: usage

    usage/getting_started.rst
    usage/graph.rst


.. raw:: latex

    \part{Extending}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Extending
    :name: extending

    extending/plugin.rst
    extending/branches.rst
    extending/best_practices.rst


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
