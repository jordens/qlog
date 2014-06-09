from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import time as pytime
import logging
import datetime

import numpy as np

from flask import Flask, jsonify, request, current_app, abort
from flask.ext import restful
from flask.ext.restful import reqparse

from sqlalchemy import create_engine, asc, desc
from sqlalchemy.orm import scoped_session, sessionmaker

from . import db


logger = logging.getLogger("qlog")


def shutdown_session(exception=None):
    current_app.db_session.remove()



class List(restful.Resource):
    def get(self, type):
        if type == "collections":
            t = db.Collection
        elif type == "variables":
            t = db.Variable
        else:
            abort(404, "No such object {}".format(type))
        return {type: [o.name for o in current_app.db_session.query(t)]}


_json_convert = {
        datetime.datetime: lambda d: d.isoformat(),
}

def to_json(obj, skip=[]):
    d = {}
    cls = obj.__class__
    for c in cls.__table__.columns:
        if c.name in skip:
            continue
        v = getattr(obj, c.name)
        if c.type in _json_convert:
            v = _json_convert[c.type](v)
        d[c.name] = v
    return d


class Collection(restful.Resource):
    def get(self, coll):
        c = current_app.db_session.query(db.Collection).filter(
                db.Collection.name == coll).first()
        if not c:
            abort(404, "Not found: {}".format(coll))
        d = to_json(c)
        d["primary_variables"] = [v.name for v in c.primary_variables]
        d["right_collections"] = [r.name for r in c.right_collections]
        return {coll: d}

    def put(self, coll):
        pass
        # TODO
        # create if not exists
        # set things

    def delete(self, coll):
        current_app.db_session.query(db.Collection).filter(
                db.Collection.name == coll).delete()
        current_app.db_session.commit()


class Variable(restful.Resource):
    parser = reqparse.RequestParser()
    parser.add_argument("name", type=str)
    parser.add_argument("type", type=str)
    parser.add_argument("unit", type=str)
    parser.add_argument("logarithmic", type=bool)
    parser.add_argument("description", type=str)
    parser.add_argument("value_precision", type=float)
    parser.add_argument("minimum", type=float)
    parser.add_argument("maximum", type=float)
    parser.add_argument("log_value_precision", type=float)
    parser.add_argument("time_precision", type=int)
    parser.add_argument("time_gar", type=int)
    parser.add_argument("aggregate_stamp", type=int)
    parser.add_argument("aggregate_age", type=int)
    parser.add_argument("delete_age", type=int)

    def get(self, var):
        v = current_app.db_session.query(db.Variable).filter(
                db.Variable.name == var).first()
        if not v:
            abort(404, "Not found: {}".format(var))
        d = to_json(v)
        if v.current is not None:
            d["current"] = {"time": v.current.time, "value": v.current.value}
        return {var: d}

    def update(self, v, a):
        for k, q in a.items():
            if q is not None:
                setattr(v, k, q)
        current_app.db_session.commit()

    def put(self, var):
        args = self.parser.parse_args()
        v = current_app.db_session.query(db.Variable).filter(
                db.Variable.name == var).first()
        if not v:
            abort(404, "Not found: {}".format(var))
        self.update(v, args)
        return self.get(var)

    def post(self, var):
        args = self.parser.parse_args()
        if args["name"] is None:
            abort(404, "Need name for creation")
        if args["type"] is None:
            abort(404, "Need type for creation")
        v = db.Variable(args["name"])
        current_app.db_session.add(v)
        self.update(v, args)
        return self.get(var)

    def delete(self, var):
        v = current_app.db_session.query(db.Variable).filter(
                db.Variable.name == var).first()
        if not v:
            abort(404, "Not found: {}".format(var))
        current_app.db_session.delete(v)
        current_app.db_session.commit()


class Data(restful.Resource):
    query = reqparse.RequestParser()
    query.add_argument("start", type=int)
    query.add_argument("stop", type=int)
    query.add_argument("average", type=int)
    query.add_argument("limit", type=int, default=1000)
    query.add_argument("offset", type=int, default=0)
    query.add_argument("statistics", type=bool, default=False)

    update = reqparse.RequestParser()
    update.add_argument("value", type=float, required=True)
    update.add_argument("time", type=int)

    def retrieve(self, var, query=False):
        args = self.query.parse_args()
        v = current_app.db_session.query(db.Variable).filter(
            db.Variable.name == var).first()
        if not v:
            abort(404, "Not found: {}".format(var))
        l = v.values
        if args["start"]:
            l = l.filter(v.value_table.time >= args["start"])
        if args["stop"]:
            l = l.filter(v.value_table.time < args["stop"])
        if query:
            return l
        l = l.order_by(desc(v.value_table.time))
        l = l.limit(args["limit"]).offset(args["offset"])
        t = v.value_table
        n = l.count()
        l = l.values(t.time, t.value)
        return args, l, n

    def get(self, var):
        args, l, n = self.retrieve(var)
        l = np.fromiter(l, [(str("time"), np.uint64), (str("value"), np.float32)], n)
        if args["average"]:
            pass
        if args["statistics"]:
            return {
                    "count": n,
                    "time_min": float(l["time"].min()),
                    "time_max": float(l["time"].max()),
                    "time_mean": float(l["time"].mean()),
                    "time_std": float(l["time"].std()),
                    "value_min": float(l["value"].min()),
                    "value_max": float(l["value"].max()),
                    "value_mean": float(l["value"].mean()),
                    "value_std": float(l["value"].std()),
            }
        else:
            return {var: dict((int(a), float(b)) for a, b in l)}

    def put(self, var):
        v = current_app.db_session.query(db.Variable).filter(
            db.Variable.name == var).one()

    def post(self, var):
        args = self.update.parse_args()
        v = current_app.db_session.query(db.Variable).filter(
            db.Variable.name == var).first()
        if not v:
            abort(404, "Not found: {}".format(var))
        v.update(value=args["value"], time=args["time"])
        current_app.db_session.commit()

    def delete(self, var):
        self.retrieve(var, query=True).delete()
        current_app.db_session.commit()


class Update(restful.Resource):
    def post(self, time=None):
        if time is None:
            time = int(pytime.time()*1e6)
        for k, v in request.values.items():
            v = float(v)
            q = current_app.db_session.query(db.Variable).filter(
                db.Variable.name == k).first()
            if not q:
                abort(404, "Not found: {}".format(k))
            q.update(value=v, time=time)
        current_app.db_session.commit()


def create_app(database):
    app = Flask(__name__)
    api = restful.Api(app)

    engine = create_engine(database, convert_unicode=True)
    db_session = scoped_session(sessionmaker(autocommit=False,
        autoflush=False, bind=engine))
    db.Base.query = db_session.query_property()
    db.Base.metadata.create_all(engine)
    app.db_session = db_session

    api.add_resource(List, "/1/list/<string:type>")
    api.add_resource(Collection, "/1/collection/<string:coll>")
    api.add_resource(Variable, "/1/variable/<string:var>")
    api.add_resource(Data, "/1/data/<string:var>")
    api.add_resource(Update, "/1/update")

    app.teardown_appcontext(shutdown_session)
    return app


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="count", default=0)
    parser.add_argument("-l", "--listen", default="0.0.0.0")
    parser.add_argument("-p", "--port", default="6881", type=int)
    parser.add_argument("-d", "--database", default="sqlite:///qlog.sqlite")
    args = parser.parse_args()

    level = [logging.CRITICAL, logging.ERROR, logging.WARN, logging.INFO,
            logging.DEBUG][args.verbose - args.quiet + 3]
    logging.basicConfig(level=level)

    app = create_app(args.database)
    app.run(host=args.listen, port=args.port,
            debug=args.verbose > args.quiet)


if __name__ == "__main__":
    main()
