from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import time
import numpy as np
import pandas as pd
import requests
from bokeh import plotting


def simple_line_plot(base, collection, name, limit, interval):
    var = requests.get("%s/variable/%s/%s" % (base, collection, name)).json()

    url = "%s/data/%s/0/%i" % (base, var["id"], limit)
    df = pd.read_json(url, "split")
    y = np.log10(df.value) if var["info"].get("logarithmic") else df.value

    plotting.output_server("QLog")
    plot = plotting.line(df.time, df.value, color="#0000FF", x_axis_type="datetime",
        tools="pan,wheel_zoom,box_zoom,resize,save,hover",
        title="", #%s/%s" % (collection, name))
        legend="%s/%s" % (collection, name))
    #plotting.xaxis()[0].axis_label = ""
    unit = var["info"].get("unit", "")
    if var["info"].get("logarithmic"):
        unit = unit + " (log10)"
    plotting.yaxis()[0].axis_label = unit

    plotting.show()

    from bokeh.objects import Glyph
    ds = [r for r in plot.renderers if isinstance(r, Glyph)][0].data_source
    while True:
        df = pd.read_json(url, "split")
        ds.data["x"] = df.time
        y = np.log10(df.value) if var["info"].get("logarithmic") else df.value
        ds.data["y"] = y
        ds._dirty = True
        plotting.session().store_obj(ds)
        time.sleep(interval)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--base",
            default="http://localhost:6881/api/1")
    parser.add_argument("-c", "--collection", default="all")
    parser.add_argument("-n", "--name", default="a")
    parser.add_argument("-l", "--limit", type=int, default=100)
    parser.add_argument("-i", "--interval", type=float, default=5)
    args = parser.parse_args()

    simple_line_plot(args.base, args.collection, args.name, args.limit,
            args.interval)


if __name__ == "__main__":
    main()
