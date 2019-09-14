Branches
========

.. todo:: Work in progress!

The :meth:`timeflux.core.branch.Branch` class is based on the main ``Node`` class. Branches are useful in several cases:

- To build complex nodes by combining other nodes
- To embed Timeflux graphs in non-Timeflux applications
- To develop and test algorithms offline (for instance in a Jupyter notebook) and thus ensuring that they will also work online

See the corresponding `unit test <https://github.com/timeflux/timeflux/blob/master/test/test_branch.py>`_ for an example.
