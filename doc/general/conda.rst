Building a conda package
========================

Folow this procedure to create a conda package for timeflux:

1. Create a build environment and add the required building packages:

   .. code-block:: console

    $ conda env create -n build-env python=3.7
    $ conda activate build-env
    $ conda install conda-build anaconda-client

2. Modify the ``conda.recipe/meta.yaml`` file accordingly, in particular to
   set the version number and which github tag will be used as a reference.

3. Clone the timeflux package and build the package from its root directory.
   You should get an output as follows:

   .. code-block:: console

    $ conda-build --channel conda-forge .
    ...
    TEST END: /Users/bob/miniconda3/envs/build-env/conda-bld/noarch/timeflux-X.Y.Z-py_0.tar.bz2
    ####################################################################################
    Resource usage summary:

    Total time: 0:01:22.1
    CPU usage: sys=0:00:00.1, user=0:00:00.2
    Maximum memory usage observed: 22.3M
    Total disk usage observed (not including envs): 3.4K


    ####################################################################################
    ...

4. Upload the package. You will need an anaconda.org account and you will need
   to log-in by command-line just before:

   .. code-block:: console

     $ anaconda login
     $ anaconda upload --user bob /Users/bob/miniconda3/envs/build-env/conda-bld/noarch/timeflux-X.Y.Z-py_0.tar.bz2


Notes
-----

The ``pylsl`` and ``python-osc`` packages work correctly only through pip packages,
and the ``python-dotenv`` package is not in the default channels, which still
complicates the installation approach. Here is a environment file that should
work:

.. code-block:: yaml

   name: my-env
   channels:
     - defaults
     - conda-forge
     # - some-channel-name  # << This will be the channel where timeflux is stored.
                            #    For the moment, it will most likely be timeflux.
                            #    In the future, it may already be in conda-forge
   dependencies:
     - python
     - timeflux
     # optional, but recommended:
     - pip
     - pip:
       - pylsl
       - python-osc
