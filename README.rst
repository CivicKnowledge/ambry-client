Ambry Client
=============

A client application for Ambry libraries. The client presents a simple interfaces that returns acessors objects,
providing a think attrubute interface on the dictionaries your normally get fro mdirectly converting the JSON
objects the library returns.

The `Client` object has these methods:

- ``list()`` Return a list of  Dataset objects, one for each dataset in the library. The Dataset objects have a limited number of fields, fewer than those returned by ``dataset()``
- ``dataset(ref)`` Return a dataset, with all of the fields. Most significantly is the ``partitions`` property, which holds records for all of the files in the dataset.
- ``partition(ref)`` Return a partition, with all of its fields.


If you use ``list()`` to get datasets, they will have a subset of the available fields, to make the reuqest faster. To get the complete list use the ``Dataset.detailed`` property to re-request all of the fields.


Example
-------

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
