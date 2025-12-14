import argparse
import logging
import matplotlib as mpl
import matplotlib.colors as mcl
import matplotlib.pyplot as plt
import numpy as np

from typing import Generator
from matplotlib.typing import ColorType

from ._tool_descriptor import Tool
from .utils.reader import Reader, units


LINE_WIDTH = 1
POINT_SIZE = 3


conversion_factors = {
    'distance':       0.001, # meters to kilometers
    'track_distance': 0.001, # meters to kilometers
    'speed':          3.6,   # m/s to km/h
    'track_speed':    3.6,   # m/s to km/h
}

converted_units = {
    'distance':       'km',
    'track_distance': 'km',
    'speed':          'km/h',
    'track_speed':    'km/h',
}


def label(field: str) -> str:
    unit = converted_units.get(field, None)
    if unit is None:
        unit = units.get(field, None)
    return f"{field} ({unit})" if unit else field


def read_data(fit_file: str, x_axis: str, y_axis: list[str], y_axis_right: list[str] = []) -> tuple[str, list, dict, dict]:
    reader = Reader(fit_file)
    if not reader.ok:
        logging.error("Failed to read fit file.")
        raise RuntimeError("Failed to read fit file.")

    xpoints: list = []
    ypoints: dict[str, list] = {y: [] for y in y_axis}
    ypoints_right: dict[str, list] = {y: [] for y in y_axis_right}

    for timestamp,record in sorted(reader.data):
        if x_axis not in record:
            logging.warning(f"X-axis field '{x_axis}' not found in record at {timestamp}. Skipping.")
            continue

        xpoints.append(record[x_axis])

        for y in y_axis:
            factor = conversion_factors.get(y, 1)
            if y not in record:
                logging.debug(f"Y-axis field '{y}' not found in record at {timestamp}. Appending None.")
                ypoints[y].append(None)
            else:
                ypoints[y].append(record[y] * factor)

        for y in y_axis_right:
            factor = conversion_factors.get(y, 1)
            if y not in record:
                logging.debug(f"Y-axis right field '{y}' not found in record at {timestamp}. Appending None.")
                ypoints_right[y].append(None)
            else:
                ypoints_right[y].append(record[y] * factor)

    return reader.metadata.get('activity_name', 'Unknown Activity'), xpoints, ypoints, ypoints_right


def colors() -> Generator[ColorType, None, None]:
    for c in mpl.color_sequences['tab10']:
        yield c
    for c in mpl.color_sequences['tab20']:
        yield c


def draw_plot(plot_type: str, plot_type_right: str, activity_name: str,
              x_axis: str, y_axis: list[str], y_axis_right: list[str],
              xpoints: list, ypoints: dict, ypoints_right: dict,
              output: str|None) -> None:
    fig, ax1 = plt.subplots()
    color = colors()

    if plot_type == 'line':
        for y in y_axis:
            ax1.plot(xpoints, ypoints[y], label=label(y), linewidth=LINE_WIDTH, color=next(color))
    elif plot_type == 'scatter':
        for y in y_axis:
            ax1.scatter(xpoints, ypoints[y], label=label(y), s=POINT_SIZE, color=next(color))

    ax1.set_xlabel(label(x_axis))
    ax1.set_ylabel(", ".join([label(y) for y in y_axis]))
    ax1.set_title(activity_name)
    ax1.legend(loc='upper left')

    ax2 = None
    if y_axis_right:
        ax2 = ax1.twinx()
        if plot_type_right == 'line':
            for y in y_axis_right:
                ax2.plot(xpoints, ypoints_right[y], label=label(y), linewidth=LINE_WIDTH, color=next(color))
        elif plot_type_right == 'scatter':
            for y in y_axis_right:
                ax2.scatter(xpoints, ypoints_right[y], label=label(y), s=POINT_SIZE, color=next(color))

        ax2.set_ylabel(", ".join([label(y) for y in y_axis_right]))
        ax2.legend(loc='upper right')

    ax1.grid(True)

    if output:
        plt.savefig(output)
    else:
        plt.show()


def main(fit_file: str,
         x_axis: str, y_axis: list[str], y_axis_right: list[str] = [],
         plot_type: str = 'line', plot_type_right: str = 'line',
         output: str|None = None) -> bool:
    logging.info(f"Plotting fit file: {fit_file}")

    logging.debug(f"X-axis: {x_axis}")
    logging.debug(f"Y-axis: {y_axis}")
    logging.debug(f"Y-axis (right): {y_axis_right}")
    logging.debug(f"Plot type: {plot_type}")
    logging.debug(f"Plot type (right y-axis): {plot_type_right}")
    logging.debug(f"Output: {output}")

    try:
        activity_name, xpoints, ypoints, ypoints_right = read_data(fit_file, x_axis, y_axis, y_axis_right)
        draw_plot(plot_type, plot_type_right, activity_name, x_axis, y_axis, y_axis_right, xpoints, ypoints, ypoints_right, output)
    except Exception as e:
        logging.error(f"Failed to plot data: {e}")
        return False

    return True


def add_argparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "plot",
        help="Plot data from the fit file."
    )
    parser.add_argument(
        "fit_file",
        help="Path to the fit file."
    )
    parser.add_argument(
        "-x", "--x-axis",
        dest="x_axis",
        help="Field to use for the x-axis.",
        required=True
    )
    parser.add_argument(
        "-y", "--y-axis",
        dest="y_axis",
        help="Field to use for the y-axis.",
        required=True,
        nargs='+'
    )
    parser.add_argument(
        "--y-right",
        dest="y_axis_right",
        help="Field to use for the y-axis on the right side.",
        nargs='+',
        default=[]
    )
    parser.add_argument(
        "-t", "--type",
        dest="plot_type",
        help="Plot type: line, scatter. Default is line.",
        choices=["line", "scatter"],
        default="line"
    )
    parser.add_argument(
        "--type-right",
        dest="plot_type_right",
        help="Plot type for right y-axis: line, scatter. Default is line.",
        choices=["line", "scatter"],
        default="line"
    )
    parser.add_argument(
        "-o", "--output",
        dest="output",
        help="Path to the output image file. If not provided, shows the plot interactively.",
        default=None
    )

tool = Tool(
    name="plot",
    description="Plot data from the fit file.",
    add_argparser=add_argparser,
    main=main
)
