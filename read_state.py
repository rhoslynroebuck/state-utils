#!/usr/bin/env python

"""
Read the shared state time series as recorded by nanover.

This file can be used as a python module or as a script.

Example when used as a module:

.. code:: python

    import read_state

    # Print the timestamps and the state updates
    from timestamp, update in iter_state_file('my_file.state'):
        print(timestamp, update)

    # Print the timestamps and the full state
    from timestamp, state in iter_full_states(iter_state_file('my_file.state')):
        print(timestamp, state)

Example when used as a script:

.. code:: bash

    # show the help
    python read_state.py --help
    
    # show the timestamps and state updates one record per line
    python read_state.py my_file.state
    # show the timestamps and the state updates in human-readable form
    python read_state.py --pretty my_file.state
    
    # show the timestamps and full states one record per line
    python read_state.py --full my_file.state
    # show the timestamps and full state in human-readable form
    python read_state.py --full --pretty my_file.state

"""

import argparse
from pprint import pprint
from nanover.mdanalysis import recordings
from nanover.protocol.state import StateUpdate
from nanover.utilities.protobuf_utilities import struct_to_dict

def iter_state_updates(unpacker: recordings.Unpacker) :
    """
    Read a binary stream and yield the state updates and their timestamps.
    """
    supported_format_versions = (2,)
    magic_number = unpacker.unpack_u64()
    if magic_number != recordings.MAGIC_NUMBER:
        raise recordings.InvalidMagicNumber
    format_version = unpacker.unpack_u64()
    if format_version not in supported_format_versions:
        raise recordings.UnsuportedFormatVersion(format_version, supported_format_versions)
    while True:
        try:
            elapsed = unpacker.unpack_u128()
            record_size = unpacker.unpack_u64()
            buffer = unpacker.unpack_bytes(record_size)
        except IndexError:
            break
        state_update = StateUpdate()
        state_update.ParseFromString(buffer)
        yield (elapsed, struct_to_dict(state_update.changed_keys))


def iter_full_states(updates):
    """
    Read a stream of timestamps and state updates and yield the timestamp and the aggregated state.
    """
    aggregate_state = {}
    for timestamp, update in updates:
        aggregate_state.update(update)
        aggregate_state = {key: value for key, value in aggregate_state.items() if value is not None}
        yield (timestamp, aggregate_state)


def iter_state_file(path):
    """
    Read a file a yield the state updates and their timestamps.

    The function yields a tuple with first the timestamp and then the state update as a dictionary. When a key is `None`, this indicates that the key needs to be removed.
    """
    with open(path, "rb") as infile:
        data = infile.read()
    unpacker = recordings.Unpacker(data)
    yield from iter_state_updates(unpacker)


def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument('--full', action='store_true', default=False, help='Display the aggregated state instead of the state updates.')
    parser.add_argument('--pretty', action='store_true', default=False, help='Display the state in a more human-readable way.')
    parser.add_argument('path', help='Path to the file to read.')
    args = parser.parse_args()

    stream = iter_state_file(args.path)
    if args.full:
        stream = iter_full_states(stream)

    for update in stream:
        if args.pretty:
            print(f'---- {update[0]} ---------')
            pprint(update[1])
        else:
            print(update)


if __name__ == '__main__':
    command_line()
