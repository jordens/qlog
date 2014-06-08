from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import time
import logging

from flask import Flask, jsonify, request, current_app
from flask.ext import restful

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from . import db


logger = logging.getLogger("qlog")


def shutdown_session(exception=None):
    current_app.db_session.remove()


class Collection(restful.Resource):
    def get(self, coll=None):
        cols = current_app.db_session.query(db.Collection)
        if coll:
            cols = cols.filter(Collection.name == coll)
        d = {}
        for c in cols:
            d[c.name] = {"origin": c.origin,
                    "description": c.description,
                    "primary_variables": [v.name for v in
                        c.primary_variables],
                    "right_collections": [r.name for r in
                        c.right_collections]}
        return jsonify(d)


class Variable(restful.Resource):
    def get(self, var=None):
        vars = current_app.db_session.query(db.Variable)
        if var:
            vars = vars.filter(db.Variable.name == var)
        d = {}
        for v in vars:
            i = v.info
            d[v.name] = {"info": {
                    "time": i.time,
                    "logarithmic": i.logarithmic,
                    "unit": i.unit,
                    "description": i.description,
                    "time_precision": i.time_precision,
                    "value_precision": i.value_precision,
                    "max_gap": i.max_gap,
                    "aggregate_older": i.aggregate_older,
                    "delete_older": i.delete_older,
                    }}
            if v.current is not None:
                d[v.name]["current"] = {
                        "time": v.current.time,
                        "value": v.current.value,
                        }
        return jsonify(d)


class Data(restful.Resource):
    def get(self, var, offset=0, limit=1):
        v = current_app.db_session.query(db.Variable).filter(
            db.Variable.name == var).one()
        l = v.last().offset(offset).limit(limit)
        q = [[q.time, q.value] for q in l]
        d = {"columns": ["time", "value"],
                "index": range(l.count()),
                "data": q}
        return jsonify(d)

    def put(self, var):
        v = current_app.db_session.query(db.Variable).filter(
            db.Variable.name == var).one()
        v.update(value=float(request.form["value"]))
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

    api.add_resource(Collection,
            "/api/1/collection",
            "/api/1/collection/<string:coll>")
    api.add_resource(Variable,
             "/api/1/variable",
             "/api/1/variable/<string:var>")
    api.add_resource(Data,
            "/api/1/data/<string:var>",
            "/api/1/data/<string:var>/<int:offset>/<int:limit>")

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
