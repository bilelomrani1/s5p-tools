from matplotlib import pyplot as plt
import matplotlib.ticker


def _get_resize_event_function(ax, cbar_ax, location, width, height):
    """
    Returns a function to automatically resize the colorbar
    for cartopy plots

    Parameters
    ----------
    ax : axis
    cbar_ax : colorbar axis

    Example
    -------
        import cartopy.crs as ccrs
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(10,5), subplot_kw={'projection': ccrs.PlateCarree()})
        cbar_ax = fig.add_axes([0, 0, 0.1, 0.1])

        [... your code generating a scalar mappable ...]

        resize_colorbar = get_resize_event_function(ax, cbar_ax)
        fig.canvas.mpl_connect('resize_event', resize_colorbar)

    Credits
    -------
    Solution by pelson at http://stackoverflow.com/a/30077745/512111
    """

    def resize_color_bar(event):
        plt.draw()
        posn = ax.get_position()
        cbar_ax.set_position([posn.x0 + posn.width + location[0] - 1, posn.y0 + location[1],
                              width, posn.height - 1 + height])

    return resize_color_bar


def color_bar(fig, ax, label, location, width, height):

    cbformat = matplotlib.ticker.ScalarFormatter(
        useMathText=True)  # create the formatter
    cbformat.set_scientific(True)
    cbformat.set_powerlimits((0, 0))

    cbar_ax = fig.add_axes([location[0], location[1], width, height])
    cbar = plt.colorbar(cax=cbar_ax, format=cbformat, pad=20)
    cbar.ax.tick_params(width=1)
    tick_locator = matplotlib.ticker.MaxNLocator(steps=[1, 2, 5, 10])
    cbar.locator = tick_locator
    cbar.update_ticks()

    cbar.set_label('{label}'.format(label=label), labelpad=20)
    cbar.ax.yaxis.set_offset_position('left')
    cbar.update_ticks()

    cbar.ax.yaxis.set_ticks([], minor=True)

    resize_color_bar = _get_resize_event_function(
        ax, cbar_ax, location, width, height)
    fig.canvas.mpl_connect('resize_event', resize_color_bar)

    return cbar_ax


def adjust_color_bar_to_plot(ax, cbar_ax, location, width, height):

    resize_color_bar = _get_resize_event_function(
        ax, cbar_ax, location, width, height)
    resize_color_bar(None)
