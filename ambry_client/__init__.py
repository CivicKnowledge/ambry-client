"""Value Types for Census codes, primarily geoids.

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from collections import OrderedDict, Mapping
import requests
from six import iterkeys

# http://pypi.python.org/pypi/layered-yaml-attrdict-config/12.07.1
class AttrDict(OrderedDict):
    """An ordered dictionary with a property interface to all keys"""
    def __init__(self, *argz, **kwz):
        super(AttrDict, self).__init__(*argz, **kwz)

    def __setitem__(self, k, v):
        super(AttrDict, self).__setitem__(k, AttrDict(v) if isinstance(v, Mapping) else v)

    def __getattr__(self, k):
        if not (k.startswith('__') or k.startswith('_OrderedDict__')):
            return self[k]
        else:
            return super(AttrDict, self).__getattr__(k)

    def __setattr__(self, k, v):
        if k.startswith('_OrderedDict__'):
            return super(AttrDict, self).__setattr__(k, v)
        self[k] = v

    def __iter__(self):
        return iterkeys(super(OrderedDict, self))


    @property
    def dict(self):
        root = {}
        val = self.flatten()
        for k, v in val:
            dst = root
            for slug in k[:-1]:
                if dst.get(slug) is None:
                    dst[slug] = dict()
                dst = dst[slug]
            if v is not None or not isinstance(dst.get(k[-1]), Mapping):
                dst[k[-1]] = v

        return root

class Client(object):
    """Web client object for raw web requests"""

    list_t = "{base_url}/json"
    dataset_t = "{base_url}/json/bundle/{ref}"
    partition_t = "{base_url}/json/partition/{ref}"
    file_t = "{base_url}/file/{ref}.{ct}"

    def __init__(self, url):
        self._url = url

    def list(self):
        """ Return a list of all of the datasets in the library
        :return: dict
        """

        o = self._get(self.list_t)

        return [ Dataset( self, b ) for b in o['bundles'] ]


    def dataset(self, ref):
        """
        Return a dataset, given a vid, id, name or vname
        :param ref:
        :return:
        """

        o = self._get(self.dataset_t, ref=ref )

        return Dataset(self, o['dataset'], partitions=o['partitions'], detailed = True)


    def partition(self, ref):
        """
        Return a partition, fiven a vid, id, name or vname
        :param ref:
        :return:
        """

        o = self._get(self.partition_t, ref=ref)

        return Partition(self, o['partition'])


    def search(self, query):
        """
        Return collections of datasets and partitions that match the given search query

        :param query:
        :return:
        """

    def streamed_file(self, ref, ct):
        url = self._make_url(self.file_t, ref=ref, ct=ct)

        r = requests.get(url, stream = True)
        r.raise_for_status()

        for line in r.iter_lines():
            yield line

    def streamed_file(self, ref, ct):
        url = self._make_url(self.file_t, ref=ref, ct=ct)

        r = requests.get(url, stream=True)
        r.raise_for_status()

        for line in r.iter_lines():
            yield line


    def _get(self, template, **kwargs):
        url = self._make_url(template, **kwargs)

        r = requests.get(url)
        r.raise_for_status()
        return r.json()

    def _make_url(self, template, **kwargs):

        return template.format(base_url=self._url.strip('/'), **kwargs)

class Dataset(AttrDict):

    def __init__(self, client, d, partitions = None, detailed = False):

        super(Dataset, self).__init__(d)

        if partitions:
            self._partitions = [ Partition(client, p) for p in partitions ]
        else:
            self._partitions = None

        self.__client = client
        self.__detailed = detailed

    @property
    def partitions(self):
        if self._partitions:
            return self._partitions
        elif not self.__detailed:
            detailed = self.detailed
            return detailed.partitions
        else:
            return []

    @property
    def detailed(self):
        """
        Refetch the dataset, with all of it's details. Use when expanding a dataset entry
        from the list

        :return: Dataset """

        return self.__client.dataset(self.vid)


class Partition(AttrDict):

    def __init__(self, client,  d):
        super(Partition, self).__init__(d)

        self.__client = client

    def __iter__(self):
        import msgpack


        class StreamedBuf(object):

            def __init__(self, client, vid):
                self.g = client.streamed_file(vid, 'mpack')

            def read(self,i):
                return next(self.g)

        buf = StreamedBuf(self.__client, self.vid)

        unpacker = msgpack.Unpacker(buf)
        for unpacked in unpacker:
            yield unpacked


    @property
    def csv_lines(self):
        """Return data, as CSV rows"""

        for row in self.__client.streamed_file(self.vid, 'csv'):
            yield row


        unpacker = msgpack.Unpacker(buf)
        for unpacked in unpacker:
            print unpacked


    def write_csv(self, path):
        """Write CSV data to a file or fle-like object"""
        import os

        with open(path, 'w') as f:
            for row in self.__client.streamed_file(self.vid, 'csv'):
                f.write(row)
                f.write(os.linesep)

