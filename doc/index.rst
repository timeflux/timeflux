Timeflux documentation
======================

Timeflux is a free and open-source framework for the acquisition and real-time processing of biosignals.
Use it to bootstrap your research, build brain-computer interfaces, closed-loop biofeedback applications, interactive installations, and more. Written in Python, it works on Linux, MacOS and Windows. Made for researchers and hackers alike.

It comes with integrated communication protocols (Lab Streaming Layer, ZeroMQ, OSC), HDF5 file handling (saving and replay) and generic data manipulation tools.

Currently available plugins include signal processing nodes, machine learning tools and a JavaScript API for precise stimulus presentation and bidirectional streaming. A signal monitoring interface is included and is accessible directly from your browser.

Drivers for open and proprietary hardware (EEG, ECG, PPG, EDA, respiration, eye tracking, etc.) have already been developed, with more coming. And if your equipment is compatible with LSL, you are already good to go!

.. admonition:: What now?

    If you are new to Timeflux, start by reading the :ref:`Core concepts <concepts>` section and then follow the :ref:`getting_started` guide and the :ref:`hello_world` tutorial.

.. attention::
    Right now, the documentation is a bit coarse, and some parts of the code need polishing. We are working on it! Meanwhile, do not hesitate to :ref:`get in touch <help>`, we will be glad to help.


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
    usage/hello_world.rst
    usage/use_case.rst
    usage/going_further.rst


.. raw:: latex

    \part{Extending}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Extending
    :name: extending

    extending/plugins.rst
    extending/branches.rst
    extending/best_practices.rst


.. raw:: latex

    \part{Plugins}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Plugins
    :name: plugins

    DSP <https://doc.timeflux.io/projects/timeflux-dsp>
    UI <https://doc.timeflux.io/projects/timeflux-ui>
    rASR <https://doc.timeflux.io/projects/timeflux-rasr>
    Example <https://doc.timeflux.io/projects/timeflux-example>
    OpenBCI <https://doc.timeflux.io/projects/timeflux-openbci>
    BrainFlow <https://doc.timeflux.io/projects/timeflux-brainflow>
    HackEEG <https://doc.timeflux.io/projects/timeflux-hackeeg>
    BITalino <https://doc.timeflux.io/projects/timeflux-bitalino>
    Plux <https://doc.timeflux.io/projects/timeflux-plux>

.. raw:: latex

    \part{Reference}


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Reference
    :name: reference

    API <api/index>