from __future__ import (absolute_import, print_function,
                unicode_literals, division)

from qlog import *


def main():
    import argparse
    import logging
    import time

    import dateutil.parser
    import numpy as np

    from sqlalchemy import create_engine, orm

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", type=str,
        default="sqlite:///qlog.sqlite")
    parser.add_argument("-c", "--collection", type=str, default="all")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="count", default=0)
    parser.add_argument("-b", "--start", type=dateutil.parser.parse,
            default=None)
    parser.add_argument("-e", "--stop", type=dateutil.parser.parse,
            default=None)
    parser.add_argument("actions", nargs="+")
    args = parser.parse_args()

    level = [logging.CRITICAL, logging.ERROR, logging.WARN, logging.INFO,
            logging.DEBUG][args.verbose - args.quiet + 3]
    logging.basicConfig(level=level)
    #color_log_setup(level=level)

    echo = args.verbose - args.quiet > 0
    engine = create_engine(args.database, echo=echo)
    Base.metadata.create_all(engine)
    Session = orm.sessionmaker(bind=engine)
    session = Session()
    collection = session.query(Collection).filter(
            Collection.name == args.collection).first()
    if collection is None:
        collection = Collection(name=args.collection)
        session.add(collection)

    for action in args.actions:
        if "=" in action:
            k, v = action.split("=")
            collection.get(k.strip(), create=True).value = float(v)
        elif action == "?":
            print(", ".join("%s = %g" % (v.name, v.value) for v in 
                    collection.variables()))
        elif action.endswith("?"):
            n = action.rstrip("?").strip()
            print("time, %s" % n)
            var = collection.get(n)
            if args.start:
                args.start = time.mktime(args.start.timetuple())*1e6 + t.microsecond
            if args.stop:
                args.stop = time.mktime(args.stop.timetuple())*1e6 + t.microsecond
            for t, v in var.iterhistory(args.start, args.stop):
                print("%s, %g" % (t, v))
        elif "," in action:
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots()
            for n in action.split(","):
                n = n.strip()
                if not n:
                    continue
                var = collection.get(n)
                t, v = np.array(list(var.iterhistory(args.start, args.stop))).T
                ax.plot(t, v, label=n)
                if var.info.logarithmic:
                    ax.set_yscale("log")
            fig.autofmt_xdate()
            ax.legend()
            fig.savefig("%s_%s.pdf" % (args.collection, action))
        else:
            k = action.strip()
            print("%g" % collection.get(k).value)
    session.commit()

if __name__ == "__main__":
    main()
