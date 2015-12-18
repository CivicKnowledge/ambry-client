import unittest
from ambry_client import Client

class BasicTests(unittest.TestCase):

    library_url = None

    @classmethod
    def setUpClass(cls):
        import os

        cls.library_url = os.getenv('AMBRY_LIBRARY_URL')

        assert cls.library_url is not None, 'Must set AMBRY_LIBRARY_URL environmental variable'

        print "Testing library at :", cls.library_url
    def test_something(self):

        c = Client(self.library_url)

        for d in c.list():
            print d.vid, d.name
            d = d.detailed

            for p in d.partitions:
                print '   ', p.name, p.description


        p = c.partition('p04O002001')

        print p.name
        print p.description

        p.write_csv('/tmp/foo.csv')

        for r in p:
            print r



if __name__ == '__main__':
    unittest.main()
