#!/usr/bin/env python3
"""
Multi-Format CAN Log to CSV Converter

Converts CAN bus log files from various tools (BusMaster, PCAN-View, CL2000)
to CSV format using DBC files for signal decoding.

Supported formats:
- BusMaster Ver 3.2.2: "09:25:06:1260 Rx 1 0x136 x 8 13 24 C2 A1 00 00 90 FF"
- PCAN-View v4.2.1.533: "36    92.943 DT     00E3 Rx 8  FF 64 04 28 C6 58 49 08"
- CL2000: "Timestamp;Type;ID;Data"

Author: Naccini Marco
Version: 2.0.0
"""

import re
import csv
import argparse
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import datetime
from datetime import datetime, timedelta
from pathlib import Path

from enum import Enum

TIME_STAMP_OUTPUT = "%d.%m.%Y %H:%M:%S.%f"


class log_formats(Enum):
    """Supported log file formats"""
    BUSMASTER_3_2_2 = int(0)
    PCANView_4_2_1_533 = int(1)
    CL2000 = int(2)

class signal_name_mode(Enum):
    SIGNAL_NAME = int(0)
    MSG_NAME___SIGNAL_NAME = int(1)

class setups:
    delimiter = ';'
    signal_name = signal_name_mode.SIGNAL_NAME
    msg_counter_signal = False
    msg_pulser_signal = False

SETUP = setups

@dataclass
class CANMessage:
    """Represents a parsed CAN message from log file"""
    timestamp: str
    direction: str          # Tx/Rx
    channel: int
    can_id: int
    extended: bool          # Extended frame flag
    dlc: int               # Data Length Code
    data: bytes


@dataclass
class DBCSignal:
    """Represents a DBC signal definition"""
    name: str
    start_bit: int
    size: int
    is_little_endian: bool
    is_signed: bool
    factor: float
    offset: float
    minimum: float
    maximum: float
    unit: str


@dataclass
class DBCMessage:
    """Represents a DBC message definition"""
    can_id: int
    extended: bool
    name: str
    dlc: int
    signals: Dict[str, DBCSignal]
    counter: int
    pulse: bool


class DBCParser:
    """Parser for DBC (Database CAN) files"""

    def __init__(self):
        self.messages: Dict[int, DBCMessage] = {}

    def parse_files(self, dbc_files: List[str]):
        """Parse one or more DBC files"""
        for dbc_file in dbc_files:
            self.parse_file(dbc_file)

    def parse_file(self, dbc_file: str):
        """Parse a single DBC file"""
        print(f"Parsing DBC file: {dbc_file}")

        try:
            with open(dbc_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            # Try with latin-1 encoding if utf-8 fails
            with open(dbc_file, 'r', encoding='latin-1') as f:
                content = f.read()

        # Regex patterns for DBC parsing
        # Message pattern: BO_ <ID> <MessageName>: <DLC> <Sender>
        message_pattern = r'BO_\s+(\d+)\s+(\w+)\s*:\s*(\d+)\s+(\w+)'

        # Signal pattern: SG_ <SignalName> : <StartBit>|<Size>@<Endianness><Sign> (<Factor>,<Offset>) [<Min>|<Max>] "<Unit>" <Receivers>
        signal_pattern = r'SG_\s+(\w+)\s*:\s*(\d+)\|(\d+)@([01])([+-])\s*\(\s*([-+]?\d*\.?\d*)\s*,\s*([-+]?\d*\.?\d*)\s*\)\s*\[\s*([-+]?\d*\.?\d*)\s*\|\s*([-+]?\d*\.?\d*)\s*\]\s*"([^"]*)"\s*(.*)'

        lines = content.split('\n')
        current_message = None

        for line in lines:
            line = line.strip()

            # Parse message
            message_match = re.match(message_pattern, line)
            if message_match:
                can_id = int(message_match.group(1))
                extended = int(can_id > 2047)
                if can_id>= 0x80000000:
                    can_id = can_id & 0x1FFFFFFF

                name = message_match.group(2)
                dlc = int(message_match.group(3))
                current_message = DBCMessage(can_id=can_id, name=name, dlc=dlc, extended=extended, signals={}, counter = 0, pulse=False)
                self.messages[can_id] = current_message
                continue

            # Parse signal
            signal_match = re.match(signal_pattern, line)
            if signal_match and current_message:
                signal_name = signal_match.group(1)
                start_bit = int(signal_match.group(2))
                size = int(signal_match.group(3))
                is_little_endian = signal_match.group(4) == '1'
                is_signed = signal_match.group(5) == '-'

                try:
                    factor = float(signal_match.group(6)) if signal_match.group(6) else 1.0
                    offset = float(signal_match.group(7)) if signal_match.group(7) else 0.0
                    minimum = float(signal_match.group(8)) if signal_match.group(8) else 0.0
                    maximum = float(signal_match.group(9)) if signal_match.group(9) else 0.0
                except ValueError:
                    factor, offset, minimum, maximum = 1.0, 0.0, 0.0, 0.0

                unit = signal_match.group(10)

                signal = DBCSignal(
                    signal_name, start_bit, size, is_little_endian,
                    is_signed, factor, offset, minimum, maximum, unit
                )

                current_message.signals[signal_name] = signal

        print(f"Parsed {len(self.messages)} messages from {dbc_file}")


class MultiFormatLogParser:
    """Parser for multiple format log files"""

    def __init__(self):
        # File type detection patterns
        self.file_type = \
        [
            r'\*\*\*BUSMASTER Ver 3\.2\.2\*\*\*',
            r';\s+Generated by PCAN-View.*',
            r'# Logger type: CL2000'
        ]

        # Start date/time patterns for each format
        self.start_date_patterns = [
            r'\*\*\*START DATE AND TIME (\d+:\d+:\d+)\s+.*\*\*\*',  # BusMaster
            r';\s+Start time:\s+(\d{2}/\d{2}/\d+ \d{2}:\d{2}:\d+\.\d+)\..*',  # PCAN-View
            r''  # CL2000 (timestamp is absolute)
        ]

        # Log line patterns for each format
        self.log_patterns = [
            # BusMaster Ver 3.2.2
            (r'(\d{2}:\d{2}:\d{2}:\d{3,4})\s+'      # timestamp
             r'(Tx|Rx)\s+'                          # direction
             r'(\d+)\s+'                            # channel
             r'(0x[0-9A-Fa-f]+)\s+'                 # CAN ID
             r'(.)\s+'                              # extended flag
             r'(\d+)\s+'                            # DLC
             r'((?:[0-9A-Fa-f]{2}\s*)*)'),          # data bytes

            # PCAN-View v4.2.1.533
            (r'^\s+\d+\)?'                          # line number
             r'\s+([\d\.]*)'                        # timestamp (relative ms)
             r'\s+([A-Za-z]*)'                      # message type
             r'\s+([0-9A-F]*)'                      # CAN ID
             r'\s+([A-Za-z]*)'                      # direction
             r'\s+(\d)'                             # DLC
             r'\s+((?:[0-9A-Fa-f]{2}\s){0,8})$'),  # data bytes

            # CL2000
            (r'(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{3});'                        # timestamp
             r'(.+);'                               # message type
             r'([0-9A-Fa-f]+);'                     # CAN ID
             r'([0-9A-Fa-f]*)')                     # data bytes
        ]

        # Parser state
        self.log_format = None
        self.log_pattern = None
        self.start_date = None
        self.start_date_format = None

    def _reset_parser_state(self):
        """Reset parser state for new file"""
        self.log_format = None
        self.log_pattern = None
        self.start_date = None
        self.start_date_format = None

    def parse_file(self, log_file: str) -> List[CANMessage]:
        """Parse log file and return a list of CAN messages"""
        messages = []

        print(f"Parsing log file: {log_file}")

        # Reset parser state
        self._reset_parser_state()

        with open(log_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                # line = line.strip()
                if not line:
                    continue

                if line_num == 3587:
                    pass

                # Detect log format and start date if not yet determined
                if not self.start_date:
                    for form, start_date_format in enumerate(self.start_date_patterns):
                        xmatch = re.match(start_date_format, line)
                        if xmatch:
                            if form == log_formats.BUSMASTER_3_2_2.value:
                                tmp_str = xmatch.group(1)
                                self.start_date_format = start_date_format
                                self.start_date = datetime.strptime(tmp_str, "%d:%m:%Y")
                                break

                            elif form == log_formats.PCANView_4_2_1_533.value:
                                tmp_str = xmatch.group(1)
                                self.start_date = datetime.strptime(tmp_str, "%d/%m/%Y %H:%M:%S.%f")
                                break

                            elif form == log_formats.CL2000.value:
                                break
                    if not xmatch:
                        continue
                elif self.start_date_format:
                    xmatch = re.match(self.start_date_format, line)
                    if xmatch:
                        if form == log_formats.BUSMASTER_3_2_2.value:
                            tmp_str = xmatch.group(1)
                            self.start_date = datetime.strptime(tmp_str, "%d:%m:%Y")
                            continue

                        elif form == log_formats.PCANView_4_2_1_533.value:
                            tmp_str = xmatch.group(1)
                            self.start_date = datetime.strptime(tmp_str, "%d/%m/%Y %H:%M:%S.%f")
                            continue
                        continue

                if not self.log_pattern:
                    for form, log_pattern in enumerate(self.log_patterns):
                        xmatch = re.match(log_pattern, line)
                        if xmatch:
                            self.log_format = int(form)
                            self.log_pattern = log_pattern
                            break
                    if not xmatch:
                        continue

                match = re.match(self.log_pattern, line)
                if match:
                    if self.log_format == log_formats.BUSMASTER_3_2_2.value:
                        tstr = match.group(1)
                        t1 = self.start_date
                        h, m, s, ms = tstr.split(":")
                        s = s + '.' + ms
                        h, m, s = int(h), int(m), float(s)
                        delta = timedelta(hours=h, minutes=m, seconds=s) #, milliseconds=ms)
                        t2 = t1 + delta
                        timestamp = t2.strftime(TIME_STAMP_OUTPUT)[:-2]
                        direction = match.group(2)
                        channel = int(match.group(3))
                        can_id = int(match.group(4), 16)  # Convert from hex
                        extended = int(match.group(5) == 'x')
                        dlc = int(match.group(6))
                        data_str = match.group(7).strip()
                    elif self.log_format == log_formats.PCANView_4_2_1_533.value:
                        tstr = match.group(1)
                        t1 = self.start_date
                        ms = float(tstr)
                        delta = timedelta(hours=0, minutes=0, seconds=0, milliseconds=ms)
                        t2 = t1 + delta
                        timestamp = t2.strftime(TIME_STAMP_OUTPUT)[:-2]
                        direction = match.group(4)
                        ID = match.group(3)
                        can_id = int(ID, 16)  # Convert from hex
                        extended = int(len(ID) > 4)
                        channel = 0
                        dlc = int(match.group(5))
                        data_str = match.group(6).strip()
                    elif self.log_format == log_formats.CL2000.value:
                        tstr = match.group(1)

                        tstr = datetime.strptime(tstr, "%Y/%m/%d-%H:%M:%S.%f")
                        timestamp = tstr.strftime(TIME_STAMP_OUTPUT)[:-3]
                        #timestamp = tstr
                        direction = 'Rx'

                        extended = int(match.group(2))
                        channel = 0
                        can_id = int(match.group(3), 16)
                        data_str = match.group(4).strip()
                        dlc = int(len(data_str)/2)

                    # Convert hex data to bytes
                    if data_str:
                        data_bytes = bytes.fromhex(data_str.replace(' ', ''))
                    else:
                        data_bytes = b''


                    #
                    #if extended and (can_id <= 0x7ff) :
                    #    can_id = (1 << 31) | can_id
                    #

                    # Verify that DLC matches data length
                    if len(data_bytes) != dlc:
                        print(f"Warning: DLC mismatch at line {line_num}: expected {dlc}, got {len(data_bytes)}")

                    message = CANMessage(
                        timestamp=timestamp,
                        direction=direction,
                        channel=channel,
                        can_id=can_id,
                        extended=extended,
                        dlc=dlc,
                        data=data_bytes)

                    messages.append(message)
                else:
                    print(f"Warning: Could not parse line {line_num}: {line}")

        print(f"Parsed {len(messages)} CAN messages")
        return messages


class SignalDecoder:
    """Signal decoder for CAN signals using DBC definitions"""

    @staticmethod
    def extract_signal_value(data: bytes, signal: DBCSignal) -> Optional[float]:
        """Extract signal value from CAN data"""
        if len(data) == 0:
            return None

        try:
            # Convert data to 64-bit integer
            data_padded = data + b'\x00' * (8 - len(data))  # Pad to 8 bytes
            data_int = int.from_bytes(data_padded, byteorder='little')

            # Calculate bit position considering endianness
            if signal.is_little_endian:
                # Intel format (little endian)
                start_bit = signal.start_bit
            else:
                # Motorola format (big endian)
                # Convert bit position from Motorola to Intel
                byte_pos = signal.start_bit // 8
                bit_pos = signal.start_bit % 8
                start_bit = byte_pos * 8 + (7 - bit_pos) - signal.size + 1

            # Extract bits
            mask = (1 << signal.size) - 1
            raw_value = (data_int >> start_bit) & mask

            # Handle sign if necessary
            if signal.is_signed and raw_value & (1 << (signal.size - 1)):
                raw_value -= (1 << signal.size)

            # Apply scaling factor and offset
            physical_value = raw_value * signal.factor + signal.offset

            return physical_value

        except Exception as e:
            print(f"Error decoding signal {signal.name}: {e}")
            return None

def get_pulser_name(DBC_message_name):
    return '_' + DBC_message_name + '_Pulser'

def get_counter_name(DBC_message_name):
    return '_' + DBC_message_name + '_Counter'


def convert_log_to_csv(log_file: str, dbc_files: List[str], output_file: str):
    """Convert CAN log file to CSV using DBC files"""

    # Parse DBC files
    dbc_parser = DBCParser()
    dbc_parser.parse_files(dbc_files)

    if not dbc_parser.messages:
        print("Error: No messages found in DBC files")
        return

    dbc_parser_messages_ID = []
    for msg in dbc_parser.messages:
        dbc_message = dbc_parser.messages[msg]

        tmp_str = str(dbc_message.can_id) + '|' + str(dbc_message.extended) + '|' + str(dbc_message.dlc)

        dbc_parser_messages_ID.append(tmp_str)

    # Parse log file
    log_parser = MultiFormatLogParser()
    can_messages = log_parser.parse_file(log_file)

    if not can_messages:
        print("Error: No CAN messages found in log file")
        return

    # Collect all unique signal names
    all_signals = set()
    for dbc_message in dbc_parser.messages.values():
        if SETUP.msg_counter_signal:
            all_signals.add(get_counter_name(dbc_message.name))
        if SETUP.msg_pulser_signal:
            all_signals.add(get_pulser_name(dbc_message.name))
        for signal_name in dbc_message.signals.keys():
            if SETUP.signal_name == signal_name_mode.SIGNAL_NAME.value:
                all_signals.add(signal_name)
            else:
                all_signals.add(dbc_message.name + '.' + signal_name)

    all_signals = sorted(list(all_signals))
    print(f"Found {len(all_signals)} unique signals")

    # Prepare CSV header
    csv_header = ['time'] + all_signals
    #csv_header.append("")

    # Process messages and write CSV
    decoder = SignalDecoder()

    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=SETUP.delimiter, lineterminator = ';\r\n')
        writer.writerow(csv_header)
        rrow = 0

        for can_message in can_messages:
            # Initialize row with timestamp and empty values
            if rrow == 0:
                row = [can_message.timestamp] + [None] * len(all_signals)
            else:
                row[0] = can_message.timestamp

            if SETUP.msg_pulser_signal:
                for CAN_id, dbc_message in enumerate(dbc_parser.messages):
                    if dbc_parser.messages[dbc_message].pulse:
                        signal_name = get_pulser_name(dbc_parser.messages[dbc_message].name)
                        signal_index = all_signals.index(signal_name) + 1  # +1 for timestamp
                        value = 0
                        if value is not None:
                            row[signal_index] = value
                    dbc_parser.messages[dbc_message].pulse = False


            # If CAN message has corresponding DBC definition
            can_message_ID = str(can_message.can_id) + '|' + str(can_message.extended) + '|' + str(can_message.dlc)

            if can_message_ID in dbc_parser_messages_ID:
                dbc_message = dbc_parser.messages[can_message.can_id]

                dbc_parser.messages[can_message.can_id].counter += 1
                dbc_parser.messages[can_message.can_id].pulse = True

                if SETUP.msg_counter_signal:
                    signal_name = get_counter_name(dbc_message.name)
                    signal_index = all_signals.index(signal_name) + 1  # +1 for timestamp
                    value = dbc_parser.messages[can_message.can_id].counter
                    if value is not None:
                        row[signal_index] = value

                if SETUP.msg_pulser_signal:
                    signal_name = get_pulser_name(dbc_message.name)
                    signal_index = all_signals.index(signal_name) + 1  # +1 for timestamp
                    value = 1
                    if value is not None:
                        row[signal_index] = value

                # Decode all signals in the message
                for signal_name, signal in dbc_message.signals.items():
                    if SETUP.signal_name == signal_name_mode.SIGNAL_NAME.value:
                        signal_name_extended = signal_name
                    else:
                        signal_name_extended = dbc_message.name + '.' + signal_name

                    if signal_name_extended in all_signals:
                        signal_index = all_signals.index(signal_name_extended) + 1  # +1 for timestamp
                        value = decoder.extract_signal_value(can_message.data, signal)
                        rrow += 1
                        if value is not None:
                            row[signal_index] = value

            #row.append("")
            writer.writerow(row)

    print(f"CSV file created: {output_file}")


def ms_2_timestamp(ms):
    """Convert milliseconds to timestamp format"""
    # Time in milliseconds

    # Convert to timedelta
    delta = datetime.timedelta(milliseconds=ms)

    # Extract hours, minutes, seconds and milliseconds
    hours, remainder = divmod(delta.total_seconds(), 3600)
    minutes, remainder = divmod(remainder, 60)
    seconds = int(remainder)
    milliseconds = int((remainder - seconds) * 1000)

    # Format the result
    formatted_time = f"{int(hours):02}:{int(minutes):02}:{seconds:02}.{milliseconds:03}"
    return formatted_time


def main():
    """
    log_file = "Veh.049_BMLogs.log"  # Your BusMaster log file
    dbc_files_list = ["MCU_TM4.dbc", "TRK_CAN1.dbc"]  # Your DBC file list
    output_csv = "out2.csv"  # Output CSV file name

    # Execute conversion
    convert_log_to_csv(log_file, dbc_files_list, output_csv)
    """

    """Main function"""
    parser = argparse.ArgumentParser(description='Converts file logs and traces into CSV using file DBC')

    parser.add_argument('log_file', help='Input CAN log file')
    parser.add_argument('dbc_files', nargs='+', help='One or more DBC files')
    parser.add_argument('-o', '--output', default='output.csv', help='Output CSV file (default: output.csv)')
    parser.add_argument('-d', '--delimiter', default=';', help='CSV delimiter (default: ;)')
    parser.add_argument('-n', '--name_mode', default='signal', help='Signal name mode: signal = signal name only, message.signal = message + signal name mode')
    parser.add_argument('-mc', '--message_counter', action='store_true', help='Increment counter signal when message appears')
    parser.add_argument('-mp', '--message_pulser', action='store_true', help='Generate pulse signal when message appears')

    args = parser.parse_args()

    # Verify that files exist
    if not Path(args.log_file).exists():
        print(f"Error: Log file {args.log_file} not found")
        return

    for dbc_file in args.dbc_files:
        if not Path(dbc_file).exists():
            print(f"Error: DBC file {dbc_file} not found")
            return

    SETUP.delimiter = args.delimiter

    if args.name_mode == 'message.signal':
        SETUP.signal_name = signal_name_mode.MSG_NAME___SIGNAL_NAME

    SETUP.msg_counter_signal = int(args.message_counter) > 0
    SETUP.msg_pulser_signal = int(args.message_pulser) > 0


    # Execute conversion
    convert_log_to_csv(args.log_file, args.dbc_files, args.output)


if __name__ == '__main__':
    main()

    # ciao