from os import makedirs

import xarray as xr
from matplotlib import pyplot as plt
import matplotlib.ticker
import matplotlib.colors as colors
import numpy as np
from itertools import product
import cartopy.crs as ccrs
import seaborn as sns
from pandas.plotting import register_matplotlib_converters

from s5p_tools.map_tools import scale_bar, color_bar, adjust_color_bar_to_plot

sns.set()
register_matplotlib_converters()


# -------------------------------------------------------------------
# ------------------------ PLOTS PARAMETERS -------------------------
# -------------------------------------------------------------------

# Figure size
MAP_FIGSIZE = (6, 6)
SERIES_FIGSIZE = (10, 4)

# Projection
MAP_PROJECTION = ccrs.Mercator()

# Color palette
COLOR_PALETTE = plt.cm.RdYlGn_r

# Font sizes
SMALL_SIZE = 8
MEDIUM_SIZE = 10
BIGGER_SIZE = 18

# Scalebar
SCALEBAR_LENGTH = 50                # in km, or 'auto': automatic length
SCALEBAR_WIDTH = 2                  # line width
SCALEBAR_LOCATION = (0.2, 0)        # relative location

# Colorbar
COLORBAR_LOCATION = (1.1, 0)
COLORBAR_HEIGHT = 0.96
COLORBAR_WIDTH = 0.05


# -------------------------------------------------------------------
# ------------------------- PLOTS FUNCTIONS -------------------------
# -------------------------------------------------------------------

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title


def plot_map(DS, layer_name, label, map_name, session_folder, val_min, val_max, log=True, format='pdf'):

    fig = plt.figure(figsize=MAP_FIGSIZE)
    ax = plt.axes(projection=MAP_PROJECTION)
    ax.outline_patch.set_edgecolor('white')

    if log:
        im = plt.pcolormesh(DS.longitude,
                            DS.latitude,
                            DS[layer_name],
                            transform=MAP_PROJECTION,
                            cmap=ccrs.PlateCarree(),
                            vmin=val_min, vmax=val_max,
                            norm=colors.LogNorm())
    else:
        im = plt.pcolormesh(DS.longitude,
                            DS.latitude,
                            DS[layer_name],
                            transform=MAP_PROJECTION,
                            cmap=ccrs.PlateCarree(),
                            vmin=val_min, vmax=val_max)

    plt.title('')
    cbar_ax = color_bar(fig, ax, label, COLORBAR_LOCATION, COLORBAR_WIDTH, COLORBAR_HEIGHT)
    scale_bar(ax, SCALEBAR_LOCATION, SCALEBAR_LENGTH, linewidth=SCALEBAR_WIDTH)

    makedirs("{folder}/maps/{couche}".format(folder=session_folder,
                                             couche=layer_name), exist_ok=True)

    adjust_color_bar_to_plot(ax, cbar_ax, COLORBAR_LOCATION, COLORBAR_WIDTH, COLORBAR_HEIGHT)

    plt.savefig("{folder}/maps/{couche}/{couche}_{region}_map.{format}".format(folder=session_folder,
                                                                          couche=layer_name,
                                                                          region=map_name,
                                                                               format=format),
                bbox_inches='tight',
                pad_inches=0)


def plot_time_series(DS, layer_name, label, map_name, session_folder, format='pdf'):

    fig = plt.figure(figsize=SERIES_FIGSIZE)
    ax = plt.axes()

    ax.yaxis.set_major_formatter(
        matplotlib.ticker.ScalarFormatter(useMathText=True))
    DS[layer_name].plot.line('x', ax=ax)
    plt.xlabel('')
    plt.ylabel(label)

    makedirs("{folder}/time_series/{couche}".format(folder=session_folder,
                                                    couche=layer_name), exist_ok=True)

    plt.savefig("{folder}/time_series/{couche}/{couche}_{region}_time_series.{format}".format(folder=session_folder,
                                                                                         couche=layer_name,
                                                                                         region=map_name,
                                                                                              format=format),
                pad_inches=0)
