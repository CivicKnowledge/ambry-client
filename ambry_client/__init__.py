""" Ambry Web Client


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
        assert not isinstance(k, list)
        super(AttrDict, self).__setitem__(k, AttrDict(v)
            if (isinstance(v, Mapping) and not isinstance(v, AttrDict)) else v)

    def __getattr__(self, k):
        assert not isinstance(k, list)
        if k.startswith('__') or k.startswith('_OrderedDict__'):
            return super(AttrDict, self).__getattr__(k)
        return self[k]

    def __setattr__(self, k, v):
        if k.startswith('__') or k.startswith('_OrderedDict__'):
            return super(AttrDict, self).__setattr__(k, v)
        self[k] = v

    def __iter__(self):
        for k in  iterkeys(super(OrderedDict, self)):
            if not k.startswith('_'):
                yield k

    def items(self):
        for k in self:
            if not k.startswith('_'):
                yield (k, self[k])

    @staticmethod
    def flatten_dict(data, path=tuple()):
        from six import iteritems
        dst = list()
        for k, v in iteritems(data):
            k = path + (k,)
            if isinstance(v, Mapping):
                for v in v.flatten(k):
                    dst.append(v)
            else:
                dst.append((k, v))
        return dst

    def flatten(self, path=tuple()):
        return self.flatten_dict(self, path=path)

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

class NotFoundError(Exception):
    pass

class Client(object):
    """Web client object for raw web requests"""

    auth_t = "{base_url}/auth"
    list_t = "{base_url}/json"
    dataset_t = "{base_url}/json/bundle/{ref}"
    partition_t = "{base_url}/json/partition/{ref}"
    file_t = "{base_url}/file/{ref}.{ct}"
    test_t = "{base_url}/auth-test"
    resolve_t = "{base_url}/resolve/{ref}"

    def __init__(self, url, username=None, secret = None):
        self._url = url

        self.username = username
        self.secret = secret

    @property
    def library(self):
        """The Library is just a subclass of the Client"""

        return Library(self._url, self.username, self.secret)

    def test(self, **kwargs):
        """
        Test the connection and authentication
        :param ref:
        :return:
        """

        return self._put(self.test_t, data=kwargs)

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

    def bundle(self, ref):
        """
        Return a bundle, given a vid, id, name or vname. A bundle is a dataset with additional interfaces
        :param ref:
        :return:
        """

        o = self._get(self.dataset_t, ref=ref)

        return Bundle(self, o['dataset'], partitions=o['partitions'], detailed=True)

    def partition(self, ref):
        """
        Return a partition, fiven a vid, id, name or vname
        :param ref:
        :return:
        """

        o = self._get(self.partition_t, ref=ref)

        return Partition(self, o['partition'])

    def resolve(self, ref):
        """Return information about either a partition or a bundle

        :param ref: Any kind of bundle or partition reference

        :return: (bundle, partition). If the ref is to a bundle, the partition will be None
        """

        o = self._get(self.resolve_t, ref=ref)

        if 'partition' in o:
            return (Bundle(self, o['bundle'], partitions=None, detailed=False),
                    Partition(self, o['partition']) )
        else:
            return (Bundle(self, o['bundle'], partitions=None, detailed=False), False )

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

        r = requests.get(url, stream=True, headers=self._headers())
        r.raise_for_status()

        for line in r.iter_lines():
            yield line


    def _headers(self, **kwargs):
        from jose import jwt

        h = {}

        if self.username and self.secret:
            t = jwt.encode({'u': self.username }, self.secret, algorithm='HS256')
            h['Authorization'] = "JWT {}:{}".format(self.username,t)

        for k, v in kwargs.items():
            k = k.replace('_', '-').capitalize()
            h[k] = v

        return h

    def _process_status(self, r):
        if r.status_code == 404:
            raise NotFoundError("Not found: {}".format(r.request.url))
        try:
            r.raise_for_status()
        except:
            raise

    def _put(self, template, data, **kwargs):
        import json
        url = self._make_url(template, **kwargs)

        r = requests.put(url, data=json.dumps(data), headers=self._headers(content_type="application/json"))
        self._process_status(r)
        return r.json()

    def _get(self, template, **kwargs):
        url = self._make_url(template, **kwargs)

        r = requests.get(url, headers=self._headers())

        self._process_status(r)
        if r.headers['content-type'] == 'application/json':
            return r.json()
        else:
            return r.content

    def _delete(self, template, **kwargs):
        url = self._make_url(template, **kwargs)

        r = requests.delete(url, headers=self._headers())
        self._process_status(r)
        if r.headers['content-type'] == 'application/json':
            return r.json()
        else:
            return r.content

    def _post_file(self, path, template, **kwargs):
        import os

        headers = self._headers(
            content_type="application/json",
            content_length=os.path.getsize(path),
            content_transfer_encoding='binary'
        )

        url = self._make_url(template, **kwargs)

        with open(path, 'rb') as f:
            r = requests.post(url, data=f, headers=headers)

        self._process_status(r)
        return r.json()

    def _make_url(self, template, **kwargs):

        return template.format(base_url=self._url.strip('/'), **kwargs)

class Dataset(AttrDict):

    def __init__(self, client, d, partitions = None, detailed = False):
        """

        :param client:
        :param d:
        :param partitions:
        :param detailed: If true, the partitions have already been loaded
        :return:
        """

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

    @property
    def bundle(self):
        """
        Refetch the dataset, with all of it's details. Use when expanding a dataset entry
        from the list

        :return: Dataset """

        return self.__client.bundle(self.vid)


class Partition(AttrDict):

    def __init__(self, client,  d):

        super(Partition, self).__init__(d)

        self.__client = client

    def rows(self):
        """Return an iterator over rows of the data file. The first row is the headers.

        Unlike iterating over the CSV file, these rows will have data types that match the schema.

        FIXME: Dates are probably broken, though.
        """
        raise Exception()
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

    def write_csv(self, path, cb=None):
        """Write CSV data to a file or fle-like object"""
        import os

        with open(path, 'w') as f:
            for i, row in enumerate(self.__client.streamed_file(self.vid, 'csv')):
                f.write(row)
                f.write(os.linesep)

                if i % 10000 == 0 and cb:
                    cb(i)



    def write_json_meta(self, path):
        """Write CSV data to a file or file-like object"""
        import os
        import json

        with open(path, 'w') as f:
            json.dump(self.dict, f, indent=4)


from .bundle import Bundle
from .library import Library