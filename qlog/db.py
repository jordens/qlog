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
    def __tablename__(cls):  # pylint: disable-msg=E0213
        return cls.__name__.lower()  # pylint: disable-msg=E1101

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

    @info.setter  # pylint: disable-msg=E1101
    def info(self, info):  # pylint: disable-msg=E0102
        self.infos.append(info)  # pylint: disable-msg=E1101

    def update(self, value=None, time=None):
        if time is None:
            time = datetime.datetime.now()
        v = Value(value, time)
        self.values.append(v)  # pylint: disable-msg=E1101
        return v

    def last(self):
        return self.values.order_by(desc(Value.time))

    def history(self, begin=None, end=None):
        v = self.values
        if begin:
            v = v.filter(Value.time >= begin)  # pylint: disable-msg=E1101
        if end:
            v = v.filter(Value.time < end)  # pylint: disable-msg=E1101,E1103
        return v.order_by(asc(Value.time))

    def iterhistory(self, begin=None, end=None):
        return ((v.time, v.value) for v in
                self.history(begin, end))

    @hybrid_property
    def current(self):
        return self.values.order_by(desc(Value.time)).first()

    @current.setter  # pylint: disable-msg=E1101
    def current(self, value):  # pylint: disable-msg=E0102
        self.values.append(value)  # pylint: disable-msg=E1101

    # no association_proxy since we add a new value
    @hybrid_property
    def value(self):
        # raises AttributeError if unset
        return self.current.value

    @value.setter  # pylint: disable-msg=E1101
    def value(self, value):  # pylint: disable-msg=E0102
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


class Collection(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    origin = Column(String(255))
    description = Column(String(255))
    variables_list = relationship(Variable,
            # pylint: disable-msg=E1101
            secondary=CollectionVariable.__table__,
            lazy="dynamic", order_by=desc(CollectionVariable.position))
    variables = relationship(Variable,
            # pylint: disable-msg=E1101
            secondary=CollectionVariable.__table__,
            collection_class=attribute_mapped_collection("name"),
            backref=backref("collections", lazy="dynamic"))
    values = association_proxy("variables", "value",
            creator=Variable)
    timeranges = relationship(TimeRange,
            # pylint: disable-msg=E1101
            secondary=CollectionTimeRange.__table__,
            lazy="dynamic", order_by=desc(CollectionTimeRange.position))

    def add(self, variable):
        self.variables[variable.name] = variable

    def load(self, data):
        pass
