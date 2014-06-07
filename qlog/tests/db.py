import unittest

import datetime

import numpy as np
from sqlalchemy import create_engine, orm
from qlog.db import (Variable, Value, VariableInfo,
    Collection, Base)


class DbCase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", echo=False)
        Base.metadata.create_all(self.engine)
        Session = orm.sessionmaker(bind=self.engine)
        self.session = Session()

    def test_variable(self):
        va = Variable("va")
        va.update()
        va.value = 2
        a = datetime.datetime.now()
        va.value = 4
        b = datetime.datetime.now()
        self.session.add(va)
        self.assertEqual(va.name, "va")
        self.assertEqual(va.value, 4)
        self.assertLess(va.current.time, b)
        self.assertGreater(va.current.time, a)

    def test_info(self):
        va = Variable("va")
        self.session.add(va)
        va.info = VariableInfo(time=datetime.datetime.now())
        va.info.description = "foo"
        self.assertEqual(list(va.infos), [va.info])

    def test_history(self):
        va = Variable("va")
        self.session.add(va)
        for i in range(10):
            if i == 5:
                a = datetime.datetime.now()
            va.value = i
        b = datetime.datetime.now()
        self.assertEqual([v for _, v in va.iterhistory()], range(10))
        self.assertEqual([v for _, v in va.iterhistory(a, b)], range(5, 10))

    def test_float(self):
        va = Variable("va", 4)
        self.session.add(va)
        self.assertEqual(float(va), 4)

    def test_collection(self):
        v = [Variable(str(i), i) for i in range(3)]
        c = Collection(name="test")
        self.assertEqual(c.name, "test")
        map(c.add, v)
        self.session.add(c)
        self.assertEqual(set(c.variables_list), set(v))
        for i in range(len(v)):
            self.assertEqual(c.values[str(i)], i)
            c.values[str(i)] += 1
            self.assertEqual(c.values[str(i)], i + 1)
        del c.values["0"]
        self.assertNotIn("0", c.values)
        self.assertNotIn("0", c.variables)
        self.assertNotIn("0", c.variables_list)
        del c.variables["1"]
        self.assertNotIn("1", c.values)
        self.assertNotIn("1", c.variables)
        self.assertNotIn("1", c.variables_list)

    def test_collection_load(self):
        c1, c2, c3, c4 = [Collection(name="test%i" % i) for i in range(4)]
        self.session.add(c1)
        c1.children.append(c2)
        c1.children.append(c3)
        c2.children.append(c4)
        self.assertEqual(set(c1.children), set((c2, c3)))
        self.assertEqual(set(c2.children), set((c4,)))
        self.assertEqual(set(c3.children), set())
        self.assertEqual(set(c4.children), set())

    def test_load_reacarray(self):
        now = datetime.datetime.utcnow()
        d = [[now, 1, 2], ["2100-01-01T00:01", 3, 4]]
        d = np.rec.fromrecords(d, dtype=[("time", "datetime64[us]"),
                ("va", "f8"), ("vb", "f8")])
        c = Collection.from_recarray(d, name="col")
        self.session.add(c)
        self.assertEqual(c.values, {"va": 3, "vb": 4})

if __name__ == "__main__":
    unittest.main()
