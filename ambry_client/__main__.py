"""Command line client to download files from an Ambry library

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from __future__ import print_function
import argparse
from . import Client
import os

parser = argparse.ArgumentParser(prog='ambrydl',description='Ambry library download client')

parser.add_argument('-l', '--list', default=False, action='store_true',
                    help='List, rather than download')

parser.add_argument('-d', '--dir', default=False, help='Target directory')

group = parser.add_mutually_exclusive_group()
group.add_argument('-v', '--vid',  action='store_true', help='Write to a flat dir structure using vid')
group.add_argument('-n', '--vname',  action='store_true', help='Write to a flat dir structure using vname')

parser.add_argument('url', nargs=1, type=str, help='Library URL')

parser.add_argument('partitions', nargs=argparse.REMAINDER,
                    help='References to partitions to download. Specify one or more vid, id, vname or name for a bundle or partition')

args = parser.parse_args()

client = Client(args.url[0])

def should_download(p):

    if not args.partitions:
        return True

    refs = [p.vid,p.id,p.name,p.vname,
            p.dataset.vid, p.dataset.id, p.dataset.vname, p.dataset.name
    ]

    for ref in refs:
        if ref in args.partitions:
            return True

    return False

displayed_datasets = set()

for ds in client.list():

    for p in ds.partitions:
        p.dataset = ds

        if not should_download(p):
            continue

        if ds.vid not in displayed_datasets:
            displayed_datasets.add(ds.vid)
            print(ds.vid, ds.vname, ds.title)

        print('  ', p.vid, p.vname, p.description)

        if args.vid:
            ref = p.vid
        elif args.vname:
            ref = p.vname
        else:
            ref = p.cache_key

        fn = '{}.csv'.format(ref)

        if args.dir:
            fn = os.path.join(args.dir, fn)

        if not args.list:
            if not os.path.exists(fn):
                dirname = os.path.dirname(fn)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                p.write_csv(fn)
                print('  ',"Wrote:  {}".format(fn))
            else:
                print('  ',"Exists: {}".format(fn))
