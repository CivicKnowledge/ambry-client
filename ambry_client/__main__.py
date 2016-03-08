"""Command line client to download files from an Ambry library

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from __future__ import print_function
import argparse
from . import Client
import os
import sys

def download_partition(args, client, p):
    from requests.exceptions import HTTPError

    if args.vid:
        ref = p.vid
    elif args.vname:
        ref = p.vname
    else:
        ref = p.cache_key

    fn = '{}.csv'.format(ref)
    jfn = '{}.json'.format(ref)

    if args.dir:
        fn = os.path.join(args.dir, fn)
        jfn = os.path.join(args.dir, jfn)

    def cb(m):
        print(m)

    try:
        if not os.path.exists(fn):
            dirname = os.path.dirname(fn)
            if not os.path.exists(dirname):
                os.makedirs(dirname)

            p.write_csv(fn)
            p.write_json_meta(jfn)

            print('  ', "Wrote:  {}".format(fn))
        else:
            print('  ', "Exists: {}".format(fn))

    except HTTPError as e:
        print("ERROR Failed (a) to download {}: {}".format(p.vname, e))
        if os.path.exists(fn):
            os.remove(fn)
    except Exception as e:

        print("ERROR Failed (b) to download {}: {}".format(p.vname, e))

        if os.path.exists(fn):
            os.remove(fn)

def main():
    from requests.exceptions import HTTPError
    from . import NotFoundError

    parser = argparse.ArgumentParser(prog='ambrydl',description='Ambry library download client')

    parser.add_argument('-u', '--username', help='Username, for private access')
    parser.add_argument('-s', '--secret', help='API Secret, for private access')

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

    if not args.username:
        client = Client(args.url[0])
    else:
        try:
            client = Client(args.url[0], args.username, args.secret)
            client.test()
        except HTTPError as e:
            print("Connection failed: {}".format(e))
            sys.exit(1)

    displayed_datasets = set()

    # If there references given, resolve them to partition ids.

    res_refs = {}

    for ref in args.partitions:
        try:
            b, p = client.resolve(ref)
            if b.vid not in res_refs:
                res_refs[b.vid] = set()

            if p:
                res_refs[b.vid].add(p.vid)

            else:
                for p in b.partitions:
                    res_refs[b.vid].add(p.vid)

        except NotFoundError as e:
            print("Error: not found: {}: {}".format(ref, e))

    if args.partitions and not res_refs:
        # partitions were specified, but none were valid:
        print("None", args.partitions, res_refs)
        return
    elif not res_refs:
        for e in client.list():
            res_refs[e.vid] = set()

    # Print the bundle/partition list, and maybe download them
    for bundle_vid, part_set in res_refs.items():

        b = client.bundle(bundle_vid)

        print(b.vid, b.vname, b.title)

        for p in b.partitions:
            if not part_set or p.vid in part_set:
                print('  ', p.vid, p.vname, p.description.encode('ascii', 'ignore') if p.description else '')

                if not args.list:
                    download_partition(args, client, p)



main()
