DynamoDB Data Access Provider Plugin
====================================

.. image:: https://img.shields.io/badge/quality-production-green
    :target: https://curity.io/resources/code-examples/status/

.. image:: https://img.shields.io/badge/availability-bundled-green
    :target: https://curity.io/resources/code-examples/status/

A custom, Kotlin-based data access provider plugin for the Curity Identity Server.

Building the Plugin
~~~~~~~~~~~~~~~~~~~

You can build the plugin by issuing the command ``mvn package``. This will produce a JAR file in the ``target`` directory,
which can be installed.

Installing the Plugin
~~~~~~~~~~~~~~~~~~~~~

To install the plugin, copy the compiled JAR (and all of its dependencies) into the :file:`${IDSVR_HOME}/usr/share/plugins/${pluginGroup}`
on each node, including the admin node. For more information about installing plugins, refer to the `https://curity.io/docs/idsvr/latest/developer-guide/plugins/index.html#plugin-installation`_.

Required Dependencies
"""""""""""""""""""""

For a list of the dependencies and their versions, run ``mvn dependency:list``. Ensure that all of these are installed in
the plugin group; otherwise, they will not be accessible to this plug-in and run-time errors will result.

Tables, keys, and indexes
~~~~~~~~~~~~~~~~~~~~~~~~~

The folder `src/main/resources/schemas <src/main/resources/schemas>`_ contains JSON files with the required tables,
as well as their key and index schemas for the `Provisioned` AWS Billing Mode.
The included ``ProvisionedThroughput`` values are illustrative and need to be adapted to the final usage scenario.

There is also another set of JSON files for the `on-demand` Billing Mode which can be found from the `on-demand` folder
under the `schemas`.

Phone number uniqueness requirement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the phone number of an account must be unique among all accounts of the accounts table. Though it is not the
recommended behavior, from version 8.0, it is possible to lift this restriction by setting the
``se.curity:identity-server.plugin.dynamodb:unique-account-phone-number`` system property to ``false`` on all nodes.

Once done, it will be possible to have a given phone number shared by more than one of the accounts created after this
change. But note that it will no longer be possible to retrieve accounts using the ``getByPhone`` request, ``null`` will be systematically returned.

.. warning:: However, beware that, once set to ``false``, this system property should no longer be set to ``true`` or removed (as its default value is ``true``)! Indeed, doing so could lead to stale data, for instance:

  * Without the property, when an account "123" is created with a phone number, it can be requested by its phone number.

  * Then, the property is set to ``false`` and account "123" is updated.

  * If the property is finally reverted to default, then the account can again be requested by its phone number, but it will hold stale attributes.

More Information
~~~~~~~~~~~~~~~~

Please visit `curity.io`_ for more information about the Curity Identity Server.

.. _curity.io/plugins: https://support.curity.io/docs/latest/developer-guide/plugins/index.html#plugin-installation
.. _curity.io: https://curity.io/

