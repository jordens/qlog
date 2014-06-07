from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import time
import logging

from flask import Flask, jsonify, request
from flask.ext import restful

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from . import db


logger = logging.getLogger("qlog")


DATABASE = "sqlite:///qlog.sqlite"
engine = create_engine(DATABASE, convert_unicode=True)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
db.Base.query = db_session.query_property()
db.Base.metadata.create_all(engine)

app = Flask(__name__)
api = restful.Api(app)


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


@app.route("/")
def index():
    return "Try /api/a/collections"


def get_var(var, coll=None):
    v = db_session.query(db.Variable)
    if coll is None:
        v = v.get(var)
    else:
        v = v.join(db.Variable.collections
            ).filter(db.Collection.name == coll
            ).filter(db.Variable.name == var
            ).one()
    return v


def timestamp(t):
    return time.mktime(t.timetuple()) + t.microsecond*1e-6


class Collections(restful.Resource):
    def get(self):
        cols = db_session.query(db.Collection)
        return dict((c.name, {"id": c.id, "origin": c.origin,
            "description": c.description}) for c in cols)
api.add_resource(Collections,
       "/api/1/collections")


class Collection(restful.Resource):
    def get(self, coll):
        c = db_session.query(db.Collection)
        if type(coll) is type(1):
            c = c.get(coll)
        else:
            c = c.filter(db.Collection.name == coll).one()
        d = {"name": c.name, "id": c.id, "origin": c.origin,
            "description": c.description,
            "variables": [{"name": v.name, "id": v.id} for v in
                c.variables_list]}
        return jsonify(d)
api.add_resource(Collection,
        "/api/1/collection/<string:coll>",
        "/api/1/collection/<int:coll>")


class Variable(restful.Resource):
    def get(self, var, coll=None):
        v = get_var(var, coll)
        d = {"name": v.name, "id": v.id, "current": {}, "info": {}}
        if v.current is not None:
            d["current"] = {
                    "time": timestamp(v.current.time),
                    "value": v.current.value,
                    }
        if v.info is not None:
            d["info"] = {
                    "time": timestamp(v.info.time),
                    "logarithmic": v.info.logarithmic,
                    "unit": v.info.unit,
                    "description": v.info.description,
                    "time_precision": v.info.time_precision,
                    "value_precision": v.info.value_precision,
                    "max_gap": v.info.max_gap,
                    "aggregate_older": v.info.aggregate_older,
                    "delete_older": v.info.delete_older,
                    }
        return jsonify(d)
api.add_resource(Variable,
     "/api/1/variable/<string:coll>/<string:var>",
     "/api/1/variable/<int:var>")


class Data(restful.Resource):
    def get(self, var, coll=None, offset=0, limit=1):
        v = get_var(var, coll)
        l = v.last().offset(offset).limit(limit)
        q = [[timestamp(q.time), q.value] for q in l]
        d = {"columns": ["time", "value"],
                "index": range(l.count()),
                "data": q}
        return jsonify(d)

    def put(self, var, coll=None):
        v = get_var(var, coll)
        v.update(value=float(request.form["value"]))
        db_session.commit()
api.add_resource(Data,
        "/api/1/data/<int:var>",
        "/api/1/data/<string:coll>/<string:var>",
        "/api/1/data/<int:var>/<int:offset>/<int:limit>",
        "/api/1/data/<string:coll>/<string:var>/<int:offset>/<int:limit>")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="count", default=0)
    parser.add_argument("-l", "--listen", default="0.0.0.0")
    parser.add_argument("-p", "--port", default="6881", type=int)
    args = parser.parse_args()

    level = [logging.CRITICAL, logging.ERROR, logging.WARN, logging.INFO,
            logging.DEBUG][args.verbose - args.quiet + 3]
    logging.basicConfig(level=level)

    app.run(host=args.listen, port=args.port,
            debug=args.verbose > args.quiet)


if __name__ == "__main__":
    main()
