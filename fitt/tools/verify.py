import argparse
import logging

from garmin_fit_sdk import Decoder, Stream

from ._tool_descriptor import Tool


def main(fit_file: str) -> bool:
    logging.info(f"Verifying fit file: {fit_file}")

    try:
        stream = Stream.from_file(fit_file)
        decoder = Decoder(stream)

        if not decoder.check_integrity():
            logging.error("Fit file integrity check failed.")
            return False

        _, errors = decoder.read()

        if errors:
            logging.error(f"Fit file verification failed with {len(errors)} errors:")
            for error in errors:
                logging.error(f" - {error}")
            return False
    except Exception as e:
        logging.error(f"Failed to read fit file: {e}")
        return False

    logging.info("Fit file verification succeeded with no errors.")
    return True


def add_argparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "verify",
        help="Verify the fit file."
    )
    parser.add_argument(
        "fit_file",
        help="Path to the fit file."
    )

tool = Tool(
    name="verify",
    description="Verify the fit file.",
    add_argparser=add_argparser,
    main=main
)
