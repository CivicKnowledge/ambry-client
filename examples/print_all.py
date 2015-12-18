#
# Print all of the properties for each of the responses

from ambry_client import Client
import os

base_url = 'http://localhost:8080'

client = Client(base_url)

for ds in client.list():

    print '---- {} ---- '.format(ds.vname)

    for k,v in ds.items():
        print k,' = ', v

    for p in ds.partitions:
        print '    ---- {}'.format(p.name)
        for k, v in p.items():
            print '    ',k,' = ', v


