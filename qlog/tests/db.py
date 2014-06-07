import unittest

import datetime
from sqlalchemy import create_engine, orm
from qlog.db import (Variable, Value, VariableInfo,
    Collection, Base)


# pylint: disable-msg=R0904


class DbCase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", echo=False)
        Base.metadata.create_all(self.engine)
        Session = orm.sessionmaker(bind=self.engine)
        self.session = Session()

    def test_variable_variable(self):
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


if __name__ == "__main__":
    unittest.main()
