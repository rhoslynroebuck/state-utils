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

Recording files written using Narupa may contain the string "narupa" in some
state update keys. Using the `--narupa` keyword of the `read_state.py` command
line writes a new file with the occurence of "narupa" by "nanover".

"""

import argparse
from typing import Any
from pathlib import Path
from pprint import pprint
from nanover.mdanalysis import recordings
from nanover.protocol.trajectory import GetFrameResponse
from nanover.trajectory import FrameData


class Header:
    magic_number: int
    format_version: int

    def __init__(self, magic_number, format_version):
        self.magic_number = magic_number
        self.format_version = format_version

    def as_bytes(self) -> bytes:
        magic_number_bytes = self.magic_number.to_bytes(
            8, 'little', signed=False)
        format_version_bytes = self.format_version.to_bytes(
            8, 'little', signed=False)
        return magic_number_bytes + format_version_bytes


def read_header(unpacker: recordings.Unpacker) -> Header:
    supported_format_versions = (2,)
    magic_number = unpacker.unpack_u64()
    if magic_number != recordings.MAGIC_NUMBER:
        raise recordings.InvalidMagicNumber
    format_version = unpacker.unpack_u64()
    if format_version not in supported_format_versions:
        raise recordings.UnsuportedFormatVersion(
            format_version, supported_format_versions)
    return Header(magic_number, format_version)


def iter_traj_updates(unpacker: recordings.Unpacker, _header: Header):
    """
    Read a binary stream and yield the state updates and their timestamps.
    """
    while True:
        try:
            elapsed = unpacker.unpack_u128()
            record_size = unpacker.unpack_u64()
            buffer = unpacker.unpack_bytes(record_size)
        except IndexError:
            break
        get_frame_response = GetFrameResponse()
        get_frame_response.ParseFromString(buffer)
        frame_index = get_frame_response.frame_index
        frame = FrameData(get_frame_response.frame)
        yield (elapsed, frame_index, frame)


def iter_full_trajectories(updates):
    """
    Read a stream of timestamps and state updates and yield the timestamp
    and the aggregated state.
    """
    aggregate_traj = {}
    for timestamp, frame_index, update in updates:
        aggregate_traj.update(update)
        aggregate_traj = {
            key: value
            for key, value in aggregate_traj.items()
            if value is not None
        }
        yield (timestamp, aggregate_traj)


def iter_traj_file(path):
    """
    Read a file a yield the state updates and their timestamps.

    The function yields a tuple with first the timestamp and then the state
    update as a dictionary. When a key is `None`, this indicates that the
    key needs to be removed.
    """
    with open(path, "rb") as infile:
        data = infile.read()
    unpacker = recordings.Unpacker(data)
    header = read_header(unpacker)
    yield from iter_traj_updates(unpacker, header)


def copy_header(unpacker, writer) -> Header:
    header = read_header(unpacker)
    writer.write(header.as_bytes())
    return header


def recursive_replace(value, to_replace, replace_by):
    if not isinstance(value, dict):
        return value

    return {
        key.replace(to_replace, replace_by): recursive_replace(value, to_replace, replace_by)
        for key, value
        in value.items()
    }


def replace_and_copy_records(unpacker, writer, header, to_replace, replace_by):
    for timestamp, frame_index, update in iter_traj_updates(unpacker, header):
        print(type(update))
        new_update_dict = {
            key.replace(to_replace, replace_by): recursive_replace(value, to_replace, replace_by)
            for key, value
            in update.raw
        }
        writer.write(state_record_as_bytes(timestamp, new_update_dict))


def state_record_as_bytes(
    timestamp: int, update_dict: dict[str, Any]
) -> bytes:
    update = GetFrameResponse()
    update.changed_keys.update(update_dict)
    update_bytes = update.SerializeToString()
    length_bytes = len(update_bytes).to_bytes(8, 'little', signed=False)
    timestamp_bytes = timestamp.to_bytes(16, 'little', signed=False)
    return timestamp_bytes + length_bytes + update_bytes


def replace_narupa(unpacker: recordings.Unpacker, writer):
    """
    Replace every ocurence of `narupa` to `nanover` in key names.
    """
    header = copy_header(unpacker, writer)
    replace_and_copy_records(unpacker, writer, header, 'narupa', 'nanover')


def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--full', action='store_true', default=False,
        help='Display the aggregated state instead of the state updates.'
    )
    parser.add_argument(
        '--pretty', action='store_true', default=False,
        help='Display the state in a more human-readable way.'
    )
    parser.add_argument(
        '--narupa', type=Path, default=None,
        help='Write a new file where "narupa" is replaced by "nanover" in all keys.'
    )
    parser.add_argument('path', help='Path to the file to read.')
    args = parser.parse_args()

    if args.narupa is not None:
        with open(args.path, 'rb') as input_file:
            unpacker = recordings.Unpacker(input_file.read())
        with open(args.narupa, 'wb') as writer:
            replace_narupa(unpacker, writer)
    else:
        stream = iter_traj_file(args.path)
        if args.full:
            stream = iter_full_trajectories(stream)

        for update in stream:
            if args.pretty:
                print(f'---- {update[0]} ---------')
                pprint(update[1])
            else:
                print(update)


if __name__ == '__main__':
    command_line()
