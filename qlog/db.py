from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import datetime

from sqlalchemy import (Column, Integer, String, DateTime, Float,
    ForeignKey, desc, asc, Boolean)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
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


class Value(Base):
    variable_id = Column(Integer, ForeignKey("variable.id"), primary_key=True)
    time = Column(DateTime(), primary_key=True)
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
    time = Column(DateTime(), primary_key=True)
    logarithmic = Column(Boolean)
    unit = Column(String(255))
    description = Column(String(4095))
    time_precision = Column(Float)
    value_precision = Column(Float)
    max_gap = Column(Float)
    aggregate_older = Column(Float)
    delete_older = Column(Float)


class Variable(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    values = relationship(Value, lazy="dynamic",
            cascade="all, delete-orphan", passive_deletes=True)
    infos = relationship(VariableInfo, lazy="dynamic",
            cascade="all, delete-orphan", backref="variable")

    def __init__(self, name, value=None, time=None, info=None):
        self.name = name
        if value is not None:
            self.update(value, time)
        if info is None:
            info = VariableInfo()
            info.time = datetime.datetime.now()
        self.info = info

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
        v = self.last()
        if begin:
            v = v.filter(Value.time >= begin)
        if end:
            v = v.filter(Value.time < end)
        return v

    def iterhistory(self, begin=None, end=None):
        return ((v.time, v.value) for v in
                self.history(begin, end))

    @hybrid_property
    def current(self):
        return self.values.order_by(desc(Value.time)).first()

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
    collection_id = Column(Integer, ForeignKey("collection.id"),
            primary_key=True)
    variable_id = Column(Integer, ForeignKey("variable.id"),
            primary_key=True)
    position = Column(Integer)


class TimeRange(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    begin = Column(DateTime())
    end = Column(DateTime())


class CollectionTimeRange(Base):
    collection_id = Column(Integer, ForeignKey("collection.id"),
            primary_key=True)
    timerange_id = Column(Integer, ForeignKey("timerange.id"),
            primary_key=True)
    position = Column(Integer)


class CollectionCollection(Base):
    left_id = Column(Integer, ForeignKey("collection.id"),
            primary_key=True)
    right_id = Column(Integer, ForeignKey("collection.id"),
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
            secondary=CollectionTimeRange.__table__,
            order_by=desc(CollectionTimeRange.position),
            #collection_class=ordering_list("position"),
            backref=backref("collections", lazy="dynamic"))

    def variables(self):
        #right_ids = query(Collection.id).filter()
        #return object_session.query(Variable).filter_by
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
