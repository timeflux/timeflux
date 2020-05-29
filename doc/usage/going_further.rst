Going further
=============

Imports
-------

When your application becomes complex, you may want to split it into digestible and reusable parts. ``import`` is a special keyword you can use to combine multiple YAML files into one application.

A few examples are available here: `import.yaml <https://github.com/timeflux/timeflux/blob/master/test/graphs/import.yaml>`_, `import2.yaml <https://github.com/timeflux/timeflux/blob/master/test/graphs/import2.yaml>`_, `import3.yaml <https://github.com/timeflux/timeflux/blob/master/test/graphs/import3.yaml>`_. They are self-explanatory.


.. _templates:

Templates
---------

You can add logic to your `YAML` files, make your apps configurable, and manipulate variables. Timeflux uses `Jinja <https://jinja.palletsprojects.com/en/2.11.x/templates/>`__ under the hood.

Take the following `app.yaml` example:

.. code-block:: yaml

    graphs:
      - nodes:
        - id: my_node
          module: my_module
          class: {{ FOOBAR }}

Setting an environment variable and invoking Timeflux:

.. code-block:: bash

    timeflux -e FOOBAR="MyClass" app.yaml

Will render the template as:

.. code-block:: yaml

    graphs:
      - nodes:
        - id: my_node
          module: my_module
          class: MyClass

You are not limited to mere variable substitution. You have the full power of Jinja at your disposal, including control structures, macros, filters, and more.


Nodes
-----

Explore the API reference for a list of available nodes and the `test/graphs` directory of the corresponding GitHub repositories for examples.


Tools
-----

Useful tools and helpers can be found here: :obj:`timeflux.helpers`.

In particular, you may want to have a look at:

- :mod:`timeflux.helpers.viz` to generate images from applications, which is very useful to visually debug your application
- :mod:`timeflux.helpers.handler` if you need to deploy experiments on Windows systems and need to integrate with other software components
- :mod:`timeflux.helpers.lsl` to enumerate available LSL streams

If you are developing plugins:

- :mod:`timeflux.helpers.clock` has a set of functions to manipulate time
- :mod:`timeflux.helpers.testing` is useful to generate dummy data and simulate the graph scheduler for unit testing


Interfaces
----------

.. todo:: Work in progress!

The `timeflux_ui <https://github.com/timeflux/timeflux_ui>`_ plugin exposes a powerful `JavaScript API <https://github.com/timeflux/timeflux_ui/blob/master/timeflux_ui/www/common/assets/js/timeflux.js>`_ to build web apps and interact with Timeflux instances from a browser. Extensive documentation is on its way. Meanwhile, we invite you to explore the `available example apps <https://github.com/timeflux/timeflux_ui/tree/master/apps>`_.

