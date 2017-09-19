from functools import partial
from uuid import uuid4

import subprocess
import argparse
import multiprocessing
import os
import sys
import shlex
import logging
import traceback
import time


def set_logging():
    # Set logging level. Log to directory script was run from as __file__.stderr
    logging_level = logging.INFO  # Modify if you just want to focus on errors
    logging.basicConfig(filename='{}/{}.stderr'.format(
        os.path.dirname(os.path.abspath(__file__)), os.path.splitext(os.path.basename(os.path.abspath(__file__)))[0]),
        level=logging_level,
        format='{asctime} - {levelname:8s}: {message}',
        datefmt='{Y}-{m}-{d} {H}:{M}:{S}',
        stream=sys.stdout)


def get_pcap_files_recursive(top_dir):
    pcap_files = []
    for root, dirs, files in os.walk(top_dir):
        for file_name in files:
            pcap_files.append('{}{}'.format(root, file_name))
    if pcap_files:
        return pcap_files
    else:
        print 'No files exist in {}'.format(top_dir)
        exit(1)


def get_pcap_files(directory):

    pcap_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if pcap_files:
        return pcap_files
    else:
        print 'No files exist in {}'.format(directory)
        exit(1)


def get_pcap_file(file_name):
    file_path = os.path.abspath(file_name)
    result = os.path.isfile(file_path)
    if result:
        pcap_files = [file_path]
        return pcap_files
    else:
        print '{} Is not a File'.format(file_path)
        exit(1)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', help='directory containing pcap')
    parser.add_argument('-r', '--recursive', action="store_true",
                        help='find pcap recursively starting from directory provided', default=False)
    parser.add_argument('-v', '--verbose', action="store_true", help='Enable Verbose output', default=False)
    parser.add_argument('-f', '--file', help='process a single Pcap file')
    parser.add_argument('-w', '--workers', type=int, help='Number of Parallel workers to process pcap', default=1)
    parser.add_argument('-s', '--source', type=int, help='Used to supply the ROCK::sensor_id variable', default=uuid4())

    args = parser.parse_args()

    # Check for Conflicting options
    if args.directory and args.file:
        parser.error('Can only use one of the following options [-d,-f]')

    # Check for at least one valid option
    if not args.directory and not args.file:
        parser.error('Must supply either one of the following option [-d,-f]')
    return args


def get_bro_executable():
    try:
        bro_location = subprocess.check_output(shlex.split('which bro'))
        return bro_location
    except subprocess.CalledProcessError as e:
        # see if it is in the default rock location
        location = '/opt/bro/bin/bro'
        if os.path.isfile(location) and os.access(location, os.X_OK):
            return location
        # Attempt to use the find command to locate bro executible
        else:
            try:
                possible_locations = subprocess.check_output(shlex.split('find / -name bro'))
                for location in possible_locations.split():
                    if os.path.isfile(location) and os.access(location, os.X_OK):
                        return location
                print 'Could not locate bro executable with "which" or "find", perhaps add it to your path'
                exit(1)
            except subprocess.CalledProcessError as er:
                print 'Could not locate bro executable, perhaps add it to your path'
                print e
                print er
                exit(1)


def run_bro_replay(pcap, bro_path, source):
    try:
        # Bro command to replay pcap
        command = '{} -C -r {} local "ROCK::sensor_id={}"'.format(bro_path, pcap, source)
        # Run the command
        subprocess.call(shlex.split(command), stdout=subprocess.PIPE)
    except Exception:
        print 'Failed to process {}'.format(pcap)
        print 'See {}.stderr for more details'.format(os.path.splitext(__file__)[0])
        logging.error('Failed to process {}\n{}'.format(pcap, traceback.format_exc()))


def main():
    # make sure we are working in the directory of the python executable
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    # get arguments
    args = get_args()

    if args.directory:
        if args.recursive:
            # get a list of all possible pcap files in directory and sub directories
            pcap_list = get_pcap_files_recursive(args.directory)
        else:
            # get a list of all possible pcap files in directory
            pcap_list = get_pcap_files(args.directory)
    else:
        # get absolute path to single pcap file
        pcap_list = get_pcap_file(args.file)

    # locate the bro executable
    bro_path = get_bro_executable()

    # the number of threads to enable
    workers = args.workers

    # assigning the workers to a pool
    pool = multiprocessing.Pool(processes=workers)

    # map the fuction and send the data needed for the function
    result = pool.map_async(partial(run_bro_replay, bro_path=bro_path, source=args.source), pcap_list)
    if args.verbose:
        while not result.ready():
            print 'Pcap Left to Process: {}'.format(result._number_left)
            time.sleep(10)
    pool.close()
    pool.join()
    print 'Pcap processed. Rock'


main()
