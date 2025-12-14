import argparse
import logging

from garmin_fit_sdk import Decoder, Stream, Profile

from ._tool_descriptor import Tool


def main(fit_file: str) -> bool:
    logging.info(f"Printing fit file: {fit_file}")

    messages: dict[str, dict] = {}

    print()
    def mesg_listener(mesg_num: int, message: dict) -> None:
        print("----------")
        message_name = Profile['types']['mesg_num'].get(str(mesg_num)) # type: ignore
        print(f"Message: {message_name if message_name is not None else 'unknown'} ({mesg_num})")
        print(message)

        mkey = message_name if message_name is not None else str(mesg_num)
        if mkey not in messages:
            messages[mkey] = {
                'count': 0,
                'fields': set()
            }
        for field in message:
            messages[mkey]['fields'].add(field)
        messages[mkey]['count'] += 1

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

    print("\n==========\n")

    for message_name in messages:
        print(f"\"{message_name}\" - {messages[message_name]['count']} messages:")
        print(messages[message_name]['fields'])
        print()
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
