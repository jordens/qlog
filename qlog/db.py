from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import datetime

from sqlalchemy import (Column, Integer, String, DateTime, Float,
    ForeignKey, desc, asc, Boolean)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm import relationship, backref


class Tablename(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

Base = declarative_base(cls=Tablename)


class Value(Base):
    variable_id = Column(Integer, ForeignKey("variable.id"), primary_key=True)
    time = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float)

    def __init__(self, value=None, time=None):
        self.time = time
        self.value = value

    def __repr__(self):
        return "<F %s>" % self.value

    def __float__(self):
        return self.value


class VariableInfo(Base):
    variable_id = Column(Integer, ForeignKey("variable.id"), primary_key=True)
    time = Column(DateTime(timezone=True), primary_key=True)
    logarithmic = Column(Boolean)
    unit = Column(String(255))
    description = Column(String(255))
    time_precision = Column(Float)
    value_precision = Column(Float)
    max_gap = Column(Float)
    aggregate_older = Column(Float)
    delete_older = Column(Float)


class Variable(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    values = relationship(Value, lazy="dynamic", backref="variable",
            cascade="all, delete-orphan")
    infos = relationship(VariableInfo, lazy="dynamic",
            backref="variable", cascade="all, delete-orphan")

    def __init__(self, name, value=None, time=None):
        self.name = name
        if value is not None:
            self.update(value, time)

    def __repr__(self):
        return "<V %s>" % self.name

    @hybrid_property
    def info(self):
        return self.infos.order_by(desc(VariableInfo.time)).first()

    @info.setter
    def info(self, info):
        self.infos.append(info)

    def update(self, value=None, time=None):
        if time is None:
            time = datetime.datetime.now()
        v = Value(value, time)
        self.values.append(v)
        return v

    def update_series(self, values, times):
        for vi, ti in zip(values, times):
            self.update(vi, ti)

    def last(self):
        return self.values.order_by(desc(Value.time))

    def history(self, begin=None, end=None):
        v = self.values
        if begin:
            v = v.filter(Value.time >= begin)
        if end:
            v = v.filter(Value.time < end)
        return v.order_by(asc(Value.time))

    def iterhistory(self, begin=None, end=None):
        return ((v.time, v.value) for v in
                self.history(begin, end))

    @hybrid_property
    def current(self):
        return self.values.order_by(desc(Value.time)).first()

    @current.setter
    def current(self, value):
        self.values.append(value)

    # no association_proxy since we add a new value
    @hybrid_property
    def value(self):
        # raises AttributeError if unset
        return self.current.value

    @value.setter
    def value(self, value):
        self.update(value)

    def __float__(self):
        return float(self.value)


class CollectionVariable(Base):
    collection_id = Column(Integer, ForeignKey("collection.id"),
            primary_key=True)
    variable_id = Column(Integer, ForeignKey("variable.id"),
            primary_key=True)
    position = Column(Integer)


class TimeRange(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    begin = Column(DateTime(timezone=True))
    end = Column(DateTime(timezone=True))


class CollectionTimeRange(Base):
    collection_id = Column(Integer, ForeignKey("collection.id"),
            primary_key=True)
    timerange_id = Column(Integer, ForeignKey("timerange.id"),
            primary_key=True)
    position = Column(Integer)


class CollectionCollection(Base):
    parent_id = Column(Integer, ForeignKey("collection.id"),
            primary_key=True)
    child_id = Column(Integer, ForeignKey("collection.id"),
            primary_key=True)
    position = Column(Integer)


class Collection(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    origin = Column(String(255))
    description = Column(String(255))
    variables_list = relationship(Variable,
            secondary=CollectionVariable.__table__,
            lazy="dynamic", order_by=desc(CollectionVariable.position))
    variables = relationship(Variable,
            secondary=CollectionVariable.__table__,
            collection_class=attribute_mapped_collection("name"),
            backref=backref("collections", lazy="dynamic"))
    values = association_proxy("variables", "value",
            creator=Variable)
    values_list = association_proxy("variable_list", "value",
            creator=Variable)
    timeranges = relationship(TimeRange,
            secondary=CollectionTimeRange.__table__,
            lazy="dynamic", order_by=desc(CollectionTimeRange.position))
    children = relationship("Collection", 
            secondary=CollectionCollection.__table__,
            #foreign_keys=CollectionCollection.parent_id,
            primaryjoin=id==CollectionCollection.parent_id,
            secondaryjoin=id==CollectionCollection.child_id,
            backref=backref("parents", lazy="dynamic"),
            lazy="dynamic", order_by=desc(CollectionCollection.position))

    def add(self, variable):
        self.variables[variable.name] = variable

    def get(self, name):
        try:
            v = self.variables[name]
        except KeyError:
            v = Variable(name=name)
            self.add(v)
        return v

    def variables_all(self):
        return list(self.variables_list) + sum((c.variables_all() for c in
                self.children), [])

    def update_from_recarray(self, data, time_column="time"):
        time = data.field(time_column).astype("datetime64[us]")
        time = time.astype(float)/1e6
        time = map(datetime.datetime.fromtimestamp, time)
        for name in data.dtype.names:
            if name == time_column:
                continue
            v = self.get(name)
            v.update_series(data.field(name), time)

    @classmethod
    def from_recarray(cls, data, time_column="time", **kwargs):
        obj = cls(**kwargs)
        obj.update_from_recarray(data, time_column=time_column)
        return obj

    def load(self, data):
        pass
