# -*- coding: utf8 -*-
#
import logging
import time
import datetime
import dateutil.parser

import matplotlib.pyplot as plt
import numpy as np

from traits.etsconfig.api import ETSConfig
ETSConfig.toolkit = "qt4"
from traits.api import (HasTraits, Dict, Instance, List, Delegate, Str,
        on_trait_change, Float, Tuple, Trait, Enum, Bool, Button)
from traitsui.api import (View, Item, UItem, HGroup, VGroup,
        DefaultOverride, ListStrEditor, TabularEditor, TableEditor)
from traitsui.tabular_adapter import TabularAdapter
from traitsui.list_str_adapter import ListStrAdapter

from chaco.api import (Plot, ToolbarPlot, ArrayPlotData,
        ArrayDataSource,
        ScatterInspectorOverlay, add_default_axes, VPlotContainer,
        PlotAxis, Legend, add_default_grids, create_scatter_plot)
from chaco.tools.api import (ZoomTool, PanTool, LegendTool,
        ScatterInspector)
from chaco.scales.api import CalendarScaleSystem
from chaco.scales_tick_generator import ScalesTickGenerator

from enable.api import Component
from enable.component_editor import ComponentEditor

from .parameters import Base, Collection, Variable, VariableInfo, Value


logger = logging.getLogger(__name__)


class Explorer(HasTraits):
    collection_names = List(Str)
    selected_collection_name = Str
    collection = Instance(Collection)

    variable_names = List(Str)
    selected_variable_names = List(Str)
    variables = List(Instance(Variable))

    update = Button()
    commit = Button()

    plot = Instance(Component)

    view = View(
            HGroup(
                VGroup(
                    UItem("collection_names",
                        editor=ListStrEditor(title="Collections",
                            selected="selected_collection_name"),
                        ),
                    UItem("variable_names",
                        editor=ListStrEditor(title="Variables",
                        multi_select=True,
                        selected="selected_variable_names"),
                        ),
                    HGroup(
                        UItem("update"),
                        UItem("commit")
                        ),
                    VGroup(
                        #UItem("variable_info.description"),
                        ),
                    ),
                VGroup(
                    UItem("plot", editor=ComponentEditor()),
                    ),
                 layout="split",
                 ),
            resizable=True,
            title=u"Orchestra ― Parameter Explorer",
            width=800,
            )

    def __init__(self, session, **kwargs):
        HasTraits.__init__(self, **kwargs)
        self.session = session
        self._update_fired()

    def _plot_default(self):
        self.data = ArrayPlotData()
        plot = Plot(data=self.data)
        
        time_axis = PlotAxis(plot, orientation="bottom",
            tick_generator=ScalesTickGenerator(
                scale=CalendarScaleSystem()))
        plot.overlays.append(time_axis)
        value_axis = PlotAxis(plot, orientation="left")
        plot.overlays.append(value_axis)
       
        #add_default_axes(plot)
        #add_default_grids(plot)

        plot.legend.visible = True
        plot.tools.append(LegendTool(plot.legend))

        plot.overlays.append(ZoomTool(plot, zoom_factor=1.2))
        plot.tools.append(PanTool(plot))
        return plot

    def _commit_fired(self):
        self.session.commit()

    def _update_fired(self):
        self.session.rollback()
        self.collection_names[:] = [c[0] for c in
            self.session.query(Collection.name).order_by(Collection.name)]
        if self.collection:
            self._collection_changed(self.collection)

    def _selected_collection_name_changed(self, old, new):
        self.collection = self.session.query(Collection
                ).filter(Collection.name == new).one()

    def _collection_changed(self, new):
        self.variable_names[:] = [v.name for v in
                new.variables_list.order_by(Variable.name)]
        #del self.selected_variable_names[:]

    @on_trait_change("selected_variable_names[]")
    def _selected_variable_names_items_changed(self, obj, name, remove, add):
        for v in remove:
            self.plot.delplot(v)
            self.data.del_data("times_%s" % v)
            self.data.del_data("values_%s" % v)
        for v in add:
            times, values = self.load_data(v)
            self.data.set_data("times_%s" % v, times)
            self.data.set_data("values_%s" % v, values)
            plot = self.plot.plot(("times_%s" % v, "values_%s" % v),
                    type="scatter", color="auto", marker_size=2,
                    name=v)[0]
            plot.index_mapper = self.plot.index_mapper
            plot.value_mapper = self.plot.value_mapper
            print plot.index_mapper, self.plot.index_mapper
        self.plot.request_redraw()

    def load_data(self, name, begin=None, end=None):
        variable = self.collection.variables[name]
        times, values = np.array(list(variable.iterhistory(begin, end))).T
        times = np.array([time.mktime(t.timetuple()) +
            t.microsecond * 1e-6 for t in times]).astype(np.double)
        values = values.astype(np.double)
        #times = ArrayDataSource(times, sort_order="none")
        #values = ArrayDataSource(values, sort_order="none")
        return times, values


def main():
    import argparse
    from sqlalchemy import create_engine, orm
    from .color_log import color_log_setup

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", type=str,
        default="sqlite:///parameters.sqlite")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-q", "--quiet", action="count", default=0)
    args = parser.parse_args()

    level = [logging.CRITICAL, logging.ERROR, logging.WARN, logging.INFO,
            logging.DEBUG][args.verbose - args.quiet + 3]
    color_log_setup(level=level)

    echo = args.verbose - args.quiet > 0
    engine = create_engine(args.database, echo=echo)
    Base.metadata.create_all(engine)
    Session = orm.sessionmaker(bind=engine)
    session = Session()

    c = Explorer(session)
    c.configure_traits()


if __name__ == "__main__":
    main()
