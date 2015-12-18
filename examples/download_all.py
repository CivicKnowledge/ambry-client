#
# Download all of the files from a library, as CSV

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
