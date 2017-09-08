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
    logging.basicConfig(filename='%s/%s.stderr' % (os.path.dirname(os.path.abspath(__file__)), os.path.basename(__file__)),
                        level=logging_level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        stream=sys.stdout)


def get_pcap_files_recursive(top_dir):
    pcap_files = []
    for root, dirs, files in os.walk(top_dir):
        for file_name in files:
            pcap_files.append('%s%s' % (root, file_name))
    if pcap_files:
        return pcap_files
    else:
        print 'No files exist in %s' % top_dir
        exit(1)


def get_pcap_files(directory):

    pcap_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if pcap_files:
        return pcap_files
    else:
        print 'No files exist in %s' % directory
        exit(1)


def get_pcap_file(file_name):
    file_path = os.path.abspath(file_name)
    result = os.path.isfile(file_path)
    if result:
        pcap_files = [file_path]
        return pcap_files
    else:
        print '%s Is not a File' % file_path
        exit(1)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', help='directory containing pcap')
    parser.add_argument('-r', '--recursive', action="store_true",
                        help='find pcap recursively starting from directory provided', default=False)
    parser.add_argument('-v', '--verbose', action="store_true", help='Enable Verbose output', default=False)
    parser.add_argument('-f', '--file', help='process a single Pcap file')
    parser.add_argument('-w', '--workers', type=int, help='Number of Parallel workers to process pcap', default=1)

    args = parser.parse_args()

    # Check for Conflicting options
    if args.directory and args.file:
        parser.error('Can only use one of the following options [-d,-f]')

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


def run_bro_replay(pcap, bro_path):
    try:
        # Bro command to replay pcap
        command = '%s -C -r %s local' % (bro_path, pcap)
        # Run the command
        subprocess.call(shlex.split(command), stdout=subprocess.PIPE)
    except Exception:
        print 'Failed to process %s' % pcap
        print 'See %s.stderr for more details' % os.path.dirname(os.path.abspath(__file__))
        logging.error('Failed to process %s\n%s', pcap, traceback.format_exc())


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
    result = pool.map_async(run_bro_replay, (pcap_list, bro_path))
    if args.verbose:
        while not result.ready():
            print 'Pcap Left to Process: %s' % result._number_left
            time.sleep(10)
    pool.close()
    pool.join()


main()
