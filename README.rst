Ambry Client
=============

A client application for Ambry libraries. The client presents a simple interface that returns acessors objects,
providing a thin attrubute interface on the dictionaries you normally get by directly converting the JSON
objects the library returns.

Installation
------------

.. code-block:: bash

    $ pip install ambry-client


Command Line Client
-------------------

The ambry-client package also installs a command line program ``ambrydl``. Run ``ambrydl -h`` for the help message. If you want download all of the files from a library:


.. code-block:: bash

    $ ambrydl http://example.com

If you want to download a subset of the files, you can specify a subset of datasets or files using their vid or vnames. These are viewable in the web interface, or using the ``ambrydl -l`` command. These values look like:

- File vid: p03r003003
- File name: oshpd.ca.gov-pqi-pqi-county-0.0.3
- Dataset vid: d03r003
- Dataset vname: oshpd.ca.gov-pqi-0.0.3

Using one or more of these names, you can download specific files, or all of the files in a dataset:

.. code-block:: bash

    $ ambrydl http://example.com d03r003 cdph.ca.gov-hci-abuse_neglect-california

By default, the files will be written with the cache key, which is the standard path name for ambry files. You can also use the ``-v`` option to write the file names using the vid, or ``-n`` to write the files using the vname.


API
---

The `Client` object has these methods:

- ``list()`` Return a list of  Dataset objects, one for each dataset in the library. The Dataset objects have a limited number of fields, fewer than those returned by ``dataset()``
- ``dataset(ref)`` Return a dataset, with all of the fields. Most significantly is the ``partitions`` property, which holds records for all of the files in the dataset.
- ``partition(ref)`` Return a partition, with all of its fields.


If you use ``list()`` to get datasets, they will have a subset of the available fields, to make the reuqest faster. To get the complete list use the ``Dataset.detailed`` property to re-request all of the fields.

Locating Properties
-------------------

There are a lot of properties for each object, dynamically generated from the response from the Library server. To get a sense of what properties are available, you can either iterate over the Dataset or Partition object as a dictionary, using ``keys()``, ``values()`` or ``items()``, or view the JSON output on the Library website, available from the blue '{json}' buttons in the footer.

Writing Files
-------------

The Partition object have several methods for acessing row information and writing CSV files:

- ``rows`` returns an iterator, over the rows in the file. The first row is the header
- ``csvlines`` returns an iterator, iterating over CSV formatted lines.
- ``write_csv(path)`` writes the data in the partition to a CSV file.


Example
-------

Here is a simple example of iterating over all of the partitions in a library, printing out the dataset and partition titles, and writing CSV files:

.. code-block:: python

    from ambry_client import Client
    import os

    base_url = 'http://localhost:8080'

    client = Client(base_url)

    for ds in client.list():


        print ds.vid, ds.title

        for p in ds.partitions:
            print '  ', p.vid, p.description

            fn = '{}.csv'.format(p.vid)

            if not os.path.exists(fn):
                p.write_csv(fn)
