from __future__ import (absolute_import, print_function,
                unicode_literals, division)

import time
import numpy as np
import pandas as pd
import requests
from bokeh import plotting
from bokeh.objects import ColumnDataSource


class QlogPlot:
    def __init__(self, base, name, limit, ds):
        self.name = name
        self.var = requests.get("%s/variable/%s" % (base, name)).json()[name]
        self.url = "%s/data/%s?limit=%i" % (base, name, limit)
        ds.add([], "%s value" % name)
        ds.add([], "%s time" % name)
        self.update(ds)
        self.plot(ds)

    def plot(self, ds):
        unit = self.var["unit"]
        if self.var["logarithmic"]:
            unit = unit + " (log10)"
        plotting.line("%s time" % self.name, "%s value" % self.name,
                x_axis_type="datetime", source=ds,
                legend="%s (%s)" % (self.name, unit), title="")
        plotting.circle("%s time" % self.name, "%s value" % self.name,
                source=ds, size=2.,
                legend="%s (%s)" % (self.name, unit), title="")

    def update(self, ds):
        df = pd.read_json(self.url)
        y = np.array(df)
        if self.var["logarithmic"]:
            y = np.log10(y)
        ds.data["%s value" % self.name] = y
        ds.data["%s time" % self.name] = df.index


def simple_line_plot(base, names, limit, interval):
    plotting.output_server("QLog")
    plotting.hold()
    plotting.figure()
    ds = ColumnDataSource(data={})
    plots = [QlogPlot(base, name, limit, ds) for name in names]
    plotting.show()

    while True:
        time.sleep(interval)
        for plot in plots:
            plot.update(ds)
        ds._dirty = True
        plotting.session().store_obj(ds)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--base",
            default="http://localhost:6881/1")
    parser.add_argument("-l", "--limit", type=int, default=100)
    parser.add_argument("-i", "--interval", type=float, default=5)
    parser.add_argument("names", nargs="+")
    args = parser.parse_args()

    simple_line_plot(args.base, args.names, args.limit,
            args.interval)


if __name__ == "__main__":
    main()
