""" Bundle Object for the Ambry Web Client


Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""


from . import Dataset, AttrDict

files_t = '{base_url}/bundles/{vid}/build/files'
file_t = '{base_url}/bundles/{vid}/build/files/{name}'
file_content_t = '{base_url}/bundles/{vid}/build/files/{name}/content'


class File(AttrDict):
    def __init__(self, bundle, client,  d):

        super(File, self).__init__(d)
        self.__client = client
        self.__bundle = bundle

    def __getattr__(self, k):
        if k == 'content':
            return self.__client._get(file_content_t, vid=self.__bundle.vid, name=self.minor_type)
        else:
            return super(File, self).__getattr__(k)


class Files(AttrDict):
    def __init__(self, bundle, client):
        super(Files, self).__init__()
        self.__bundle = bundle
        self.__client = client

        for f in self.__client._get(files_t, vid=self.__bundle.vid)['files']:
            x = File(self.__bundle, self.__client, f)

            self[f['minor_type']] = x


class Bundle(Dataset):

    def __init__(self, client, d, partitions=None, detailed=False):

        super(Bundle, self).__init__(client, d, partitions, detailed)
        self.__client = client

    def __getattr__(self, k):
        if k == 'files':
            # Because @property on a method does not work in AttrDict
            return Files(self, self.__client)
        else:
            return super(Bundle, self).__getattr__(k)
