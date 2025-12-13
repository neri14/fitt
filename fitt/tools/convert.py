# import argparse
# import logging

# from garmin_fit_sdk import Decoder, Stream

# from ._tool_descriptor import Tool


# def main(fit_file: str) -> bool:
#     logging.info(f"Converting fit file: {fit_file}")

#     # https://www.topografix.com/gpx/1/1/

#     return False


# def add_argparser(subparsers: argparse._SubParsersAction) -> None:
#     parser = subparsers.add_parser(
#         "convert",
#         help="Convert the fit file to other formats."
#     )
#     parser.add_argument(
#         "fit_file",
#         help="Path to the fit file."
#     )
#     parser.add_argument(
#         "-o", "--output",
#         help="Path to the output file. If not provided, uses the same name as the input file changed extension.",
#         default=None
#     )
#     parser.add_argument(
#         "-f", "--format",
#         help="Output format, one of: gpx, csv. Default is gpx.",
#         choices=["gpx", "csv"],
#         default="gpx"
#     )

# tool = Tool(
#     name="convert",
#     description="Convert the fit file to other formats.",
#     add_argparser=add_argparser,
#     main=main
# )
