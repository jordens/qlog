from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import time as pytime, datetime

from sqlalchemy import (Column, Integer, String, DateTime, Float,
    ForeignKey, desc, asc, Boolean, Binary, Text, BigInteger)
from sqlalchemy.ext.declarative import (declarative_base, declared_attr,
        AbstractConcreteBase)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.collections import column_mapped_collection
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship, backref, object_session


class Tablename(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

Base = declarative_base(cls=Tablename)


class Value(AbstractConcreteBase):
    @declared_attr
    def variable_id(cls):
        return Column(Integer, ForeignKey("variable.id", ondelete="cascade"),
                primary_key=True)

    #time = Column(DateTime(6), primary_key=True)
    time = Column(BigInteger(), primary_key=True)

    def __init__(self, value=None, time=None):
        self.time = time
        self.value = value


class FloatValue(Value, Base):
    value = Column(Float)


class IntegerValue(Value, Base):
    value = Column(Integer)


class BooleanValue(Value, Base):
    value = Column(Boolean)


class TextValue(Value, Base):
    value = Column(Text(65535))


class BinaryValue(Value, Base):
    value = Column(Binary(65535))


class Variable(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    type = Column(String(255), nullable=False)
    logarithmic = Column(Boolean)
    unit = Column(String(255))
    description = Column(String(4095))
    value_precision = Column(Float)
    maximum = Column(Float)
    minimum = Column(Float)
    log_value_precision = Column(Float)
    time_precision = Column(BigInteger)
    time_gap = Column(BigInteger)
    aggregate_stamp = Column(BigInteger)
    aggregate_age = Column(BigInteger)
    delete_age = Column(BigInteger)

    float_values = relationship(FloatValue, lazy="dynamic",
            cascade="all, delete-orphan", passive_deletes=True)
    integer_values = relationship(IntegerValue, lazy="dynamic",
            cascade="all, delete-orphan", passive_deletes=True)
    string_values = relationship(TextValue, lazy="dynamic",
            cascade="all, delete-orphan", passive_deletes=True)
    boolean_values = relationship(BooleanValue, lazy="dynamic",
            cascade="all, delete-orphan", passive_deletes=True)
    binary_values = relationship(BinaryValue, lazy="dynamic",
            cascade="all, delete-orphan", passive_deletes=True)

    def __init__(self, name, value=None, time=None, type="float"):
        self.name = name
        self.type = type
        if value is not None:
            self.update(value, time)

    @property
    def value_table(self):
        t = self.type
        if t == "float":
            return FloatValue
        elif t == "int":
            return IntegerValue
        elif t == "text":
            return TextValue
        elif t == "bool":
            return BoolValue
        elif t == "binary":
            return BinaryValue
        else:
            raise ValueError(t)

    @property
    def values(self):
        t = self.type
        if t == "float":
            return self.float_values
        elif t == "int":
            return self.integer_values
        elif t == "string":
            return self.string_values
        elif t == "bool":
            return self.bool_values
        elif t == "binary":
            return self.binary_values
        else:
            raise ValueError(t)

    def update(self, value=None, time=None):
        if time is None:
            time = int(pytime.time()*1e6)
        v = self.value_table(value, time)
        self.values.append(v)
        return v

    def update_series(self, values, times):
        for vi, ti in zip(values, times):
            self.update(vi, ti)

    def last(self):
        return self.values.order_by(desc(self.value_table.time))

    def history(self, start=None, stop=None):
        v = self.last()
        if start:
            v = v.filter(Value.time >= start)
        if stop:
            v = v.filter(Value.time < stop)
        return v

    def iterhistory(self, start=None, stop=None):
        return ((v.time, v.value) for v in
                self.history(start, stop))

    @hybrid_property
    def current(self):
        return self.last().first()

    @current.setter
    def current(self, value):
        self.values.append(value)

    @hybrid_property
    def value(self):
        return self.current.value

    @value.setter
    def value(self, value):
        self.update(value)

    def __float__(self):
        return float(self.value)


class CollectionVariable(Base):
    collection_id = Column(Integer, ForeignKey("collection.id", ondelete="cascade"),
            primary_key=True)
    variable_id = Column(Integer, ForeignKey("variable.id", ondelete="cascade"),
            primary_key=True)
    position = Column(Integer)


class TimeRange(Base):
    id = Column(Integer, primary_key=True)
    collection_id = Column(Integer, ForeignKey("collection.id",
        ondelete="cascade"))
    name = Column(String(255))
    description = Column(String(4095))
    start = Column(DateTime())
    stop = Column(DateTime())


class CollectionCollection(Base):
    left_id = Column(Integer, ForeignKey("collection.id", ondelete="cascade"),
            primary_key=True)
    right_id = Column(Integer, ForeignKey("collection.id", ondelete="cascade"),
            primary_key=True)
    position = Column(Integer)


class Collection(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    origin = Column(String(255))
    description = Column(String(4095))
    primary_variables = relationship(Variable,
            secondary=CollectionVariable.__table__,
            order_by=desc(CollectionVariable.position),
            #collection_class=ordering_list("position"),
            backref=backref("collections", lazy="dynamic"))
    right_collections = relationship("Collection",
            secondary=CollectionCollection.__table__,
            primaryjoin=id==CollectionCollection.left_id,
            secondaryjoin=id==CollectionCollection.right_id,
            order_by=desc(CollectionCollection.position),
            #collection_class=ordering_list("position"),
            backref=backref("left_collections", lazy="dynamic"))
    timeranges = relationship(TimeRange,
            order_by=asc(TimeRange.start),
            cascade="all, delete-orphan", passive_deletes=True,
            backref="collection")

    def variables(self):
        return list(self.primary_variables) + sum((c.variables()
            for c in self.right_collections), [])

    def get(self, name, create=False, add=False):
        v = object_session(self).query(Variable).filter(
                Variable.name == name).first()
        if not v:
            if not create:
                return
            v = Variable(name=name)
            self.primary_variables.append(v)
        if not v in self.variables():
            if not add:
                return
            self.primary_variables.append(v)
        return v

    def update_from_recarray(self, data, time_column="time"):
        # time must be utc localized or utc naive
        time = data.field(time_column).astype("datetime64[us]")
        time = time.astype(float)/1e6
        time = map(datetime.datetime.fromtimestamp, time)
        for name in data.dtype.names:
            if name == time_column:
                continue
            v = self.get(name, create=True)
            v.update_series(data.field(name), time)

    @classmethod
    def from_recarray(cls, data, time_column="time", **kwargs):
        obj = cls(**kwargs)
        obj.update_from_recarray(data, time_column=time_column)
        return obj
