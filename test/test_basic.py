
from flask import Flask
from flask.ext.testing import LiveServerTestCase
from ambry_client import Client

import os
os.environ['AMBRY_DB'] = 'sqlite:////tmp/foo.db'


class MyTest(LiveServerTestCase):

    def setUp(self):

        self.username = 'user'
        self.secret = 'secret'

        self.setup_user()

    def create_app(self):

        from ambry_ui import app
        import ambry_ui.views
        import ambry_ui.jsonviews
        import ambry_ui.api
        import ambry_ui.user

        return app

    def setup_user(self):
        from ambry.library import Library
        from ambry.run import get_runconfig

        rc = get_runconfig()
        l = Library(rc, read_only=True, echo=False)

        act = l.find_or_new_account(self.username)
        act.secret = self.secret
        act.major_type = 'user'
        l.commit()
        l.database.close()

    def test_something(self):

        c = Client(self.get_server_url(), self.username, self.secret)

        self.assertEqual(dict(a=1,b=2), c.test(a=1,b=2))


if __name__ == '__main__':
    unittest.main()
