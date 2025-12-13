import argparse
import logging

from garmin_fit_sdk import Decoder, Stream, Profile

from ._tool_descriptor import Tool


def main(fit_file: str) -> bool:
    logging.info(f"Printing fit file: {fit_file}")

    fields = set()
    print()
    def mesg_listener(mesg_num, message):
        print("----------")
        message_name = Profile['types']['mesg_num'].get(str(mesg_num), f"unknown")
        print(f"Message: {message_name} ({mesg_num})")
        print(message)
    
    try:
        stream = Stream.from_file(fit_file)
        decoder = Decoder(stream)
        _, errors = decoder.read(mesg_listener=mesg_listener)

        if errors:
            logging.error(f"Errors decoding fit file:")
            for error in errors:
                logging.error(f" - {error}")
            return False
    except Exception as e:
        logging.error(f"Failed to read fit file: {e}")
        return False

    logging.info(f"Fields in RECORD messages:")
    for field in fields:
        logging.info(f" - {field}")
    return True



def add_argparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "print",
        help="Print all messages in the fit file."
    )
    parser.add_argument(
        "fit_file",
        help="Path to the fit file."
    )

tool = Tool(
    name="print",
    description="Print all messages in the fit file.",
    add_argparser=add_argparser,
    main=main
)
