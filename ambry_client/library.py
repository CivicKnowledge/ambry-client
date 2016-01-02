""" Library Object for the Ambry Web Client

The Library is a subclass of the CLient, with more interfaces.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""


from . import Client

class Library(Client):

    remotes_t = "{base_url}/config/remotes"
    accounts_t = "{base_url}/config/accounts"

    checkin_t = "{base_url}/bundles/{vid}/checkin"
    checkout_t = "{base_url}/bundles/{vid}/checkout"

    remove_t = "{base_url}/bundles/{ref}"

    @property
    def remotes(self):
        """Return a list of all of the remotes"""
        return self._get(self.remotes_t)['remotes']

    @remotes.setter
    def remotes(self, new_remotes):
        return self._put(self.remotes_t, data=new_remotes)['remotes']

    @property
    def accounts(self):
        """Return a list of all of the accounts, minus the secrets"""

        # Decrypt the passwords, then re-encrypt them.


        return self._get(self.accounts_t)['accounts']

    @accounts.setter
    def accounts(self, new_accounts):
        """Return a list of all of the accounts, minus the secrets"""

        return self._put(self.accounts_t, data=new_accounts)['accounts']

    def checkin(self, package, checkin_partitions=True, cb=None):
        from ambry.orm.exc import NotFoundError
        import os.path

        if not os.path.exists(package.path):
            raise NotFoundError("Database is not packaged. Create one by building, or run 'bambry package' " )

        if cb:
            def cb_one_arg(n):
                cb('Uploading bundle', n)
        else:
            cb_one_arg = None

        ds = package.package_dataset

        self._post_file(package.path, self.checkin_t, vid=ds.identity.vid)

        if False and package.library:
            from ambry.orm import Bundle
            bundle = Bundle(ds, package.library)

            for p in bundle.partitions:
                self._put_partition_fs(remote, p, cb=cb)

        return self, package.path

    def remove(self, ref, cb=None):
        from ambry.orm.exc import NotFoundError
        import os.path

        return self._delete(self.remove_t, ref=ref)


    def __str__(self):
        return self._url