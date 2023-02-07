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
on each node, including the admin node. For more information about installing plugins, refer to the `curity.io/plugins`_.

Required Dependencies
"""""""""""""""""""""

For a list of the dependencies and their versions, run ``mvn dependency:list``. Ensure that all of these are installed in
the plugin group; otherwise, they will not be accessible to this plug-in and run-time errors will result.

Tables, keys, and indexes
~~~~~~~~~~~~~~~~~~~~~~~~~

The folder `src/main/resources/schemas <src/main/resources/schemas>`_ contains JSON files with the required tables,
as well as their key and index schemas.
The included ``ProvisionedThroughput`` values are illustrative and need to be adapted to the final usage scenario.

System property
~~~~~~~~~~~~~~~

By default, the phone number of an account must be unique among all accounts of the accounts table, and leads to the
creation of a secondary item (used by ``getByPhone``). Though it is not the recommended behavior, it is possible to
lift this restriction by setting the ``se.curity:identity-server.plugin.dynamodb:unique-account-phone-number`` system
property to ``false`` on all nodes.

Once done, it will be possible to have a given phone number shared by more than one of the accounts created after this
change. But note that it will no longer be possible to request accounts using ``getByPhone``, ``null`` will be systematically returned.

However, beware that, once set to ``false``, this system property should no longer be set to ``true`` or removed (as
its default value is ``true``)! Indeed, doing so could lead to stale data:

* Once the property is ``false``:

 * Upon creation of a new account, the phone-related secondary item will not be created.

 * Accounts created before setting the property will keep their phone-related secondary item, but this item will not be updated upon modifications.

* If the property is then reverted to ``true``:

 * The new accounts to be created will then have a phone-related secondary item. So there will be a mix with accounts with and without the secondary item.

 * For an account created before reverting the property, it will not be found if requested using ``getByPhone``.

 * For an account created before setting the property to ``false``, then updated once to ``false``, it will be returned when requested using ``getByPhone`` but will contain stale attributes.

 * For an account created before setting the property to ``false``, then updated after reverting the property, the secondary item will no longer be updated as it will hold a different version from the main item. So that, if it is requested using ``getByPhone``, it will contain stale attributes.

More Information
~~~~~~~~~~~~~~~~

Please visit `curity.io`_ for more information about the Curity Identity Server.

.. _curity.io/plugins: https://support.curity.io/docs/latest/developer-guide/plugins/index.html#plugin-installation
.. _curity.io: https://curity.io/

