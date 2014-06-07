from qlog import *


def main():
    import argparse
    import logging
    import time

    import dateutil.parser
    import matplotlib.pyplot as plt
    import numpy as np

    from sqlalchemy import create_engine, orm
    #from .color_log import color_log_setup

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", type=str,
        default="sqlite:///qlog.sqlite")
    parser.add_argument("-c", "--collection", type=str, default="all")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="count", default=0)
    parser.add_argument("-b", "--begin", type=dateutil.parser.parse,
            default=None)
    parser.add_argument("-e", "--end", type=dateutil.parser.parse,
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
            collection.values[k.strip()] = float(v)
        elif action == "?":
            print(", ".join("%s = %g" % (v, k) for v, k in 
                    collection.values.iteritems()))
        elif action.endswith("?"):
            n = action.rstrip("?").strip()
            print("time, timestamp, %s" % n)
            var = collection.variables[n]
            for t, v in var.iterhistory(args.begin, args.end):
                ts = t.isoformat()
                tt = time.mktime(t.timetuple())+t.microsecond*1e-6
                print("%s, %f, %g" % (ts, tt, v))
        elif "," in action:
            fig, ax = plt.subplots()
            for n in action.split(","):
                n = n.strip()
                if not n:
                    continue
                var = collection.variables[n]
                t, v = np.array(list(var.iterhistory(args.begin,
                    args.end))).T
                ax.plot(t, v, label=n)
                if var.info and var.info.logarithmic:
                    ax.set_yscale("log")
            fig.autofmt_xdate()
            ax.legend()
            fig.savefig("%s_%s.pdf" % (args.collection, action))
        else:
            k = action.strip()
            print("%g" % collection.values[k])
    session.commit()

if __name__ == "__main__":
    main()
