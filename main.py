from asyncio import run
from collections import deque, Counter
from datetime import datetime
from time import time
from math import floor
from cloud_storage import *
from local_storage import *

FILTER_FIELD_NAMES: tuple = ('client_ip', 'code', 'url')
DEFAULT_INTERVAL: int = 1800
DEFAULT_FILTER: dict = {}
UNITS: tuple = ("KB", "MB", "GB", "TB", "PB")


def process_log(blob, time_range: tuple, filter: dict = {}, log_fields={}) -> list:

    try:

        lines = blob.decode('utf-8').splitlines()

        if filter:
            filter_indexes: dict = {}
            for i, field_name in enumerate(log_fields):
                if field_name in filter:
                    filter_indexes[i] = field_name

        # Work backwards on file, since newer entries are at the end
        for _ in range(len(lines)-1, 0, -1):
            line: str = lines[_]
            timestamp: int = int(line[:10])
            if timestamp < time_range[0]:
                break  # read too far
            if timestamp > time_range[1]:
                continue  # haven't read enough
            # Found a line in the time range
            entry: list = line.split()
            if entry[3] == "NONE/000":
                continue
            if entry[6].startswith("http:"):
                # Remove full URL from HTTP requests
                host = entry[6][7:].split('/')[0]
                if ":" in host:
                    entry[6] = host
                else:
                    entry[6] = host + ":80"
            if filter:
                match = False
                for i, field_name in filter_indexes.items():
                    if filter[field_name] in entry[i]:
                        match = True
                    else:
                        match = False
                        break
                if match:
                    yield entry
            else:
                yield entry

    except Exception as e:
        raise e


def get_data(env_vars: dict = {}) -> dict:

    splits = {'start': time()}

    settings = get_settings()
    assert settings, "Could not load settings.  Does {} exist?".format(SETTINGS_FILE)
    log_fields = tuple(settings.get('LOG_FIELDS').values())
    splits['get_settings'] = time()

    try:
        locations = get_locations()
    except Exception as e:
        raise e
    assert locations, "Could not load locations.  Does {} exist?".format(LOCATIONS_FILE)
    splits['get_locations'] = time()

    # Process parameters
    if 'action' in env_vars:
        action: str = env_vars.get('action', None)
        if action == "get_locations":
            return list(locations.keys())
        if action == "get_servers":
            location: str = env_vars.get('location')
            return list(locations[location])

    now = time()
    # Parse parameters to determine time range
    interval = int(env_vars.get('interval', settings['DEFAULT_VALUES'].get('interval', 900)))
    end_time = int(env_vars.get('end_time', floor(now)))
    start_time = int(env_vars.get('start_time', end_time - interval))
    time_range = (start_time, end_time)

    # Parse parameters to determine filter
    filter: dict = DEFAULT_FILTER
    for f in FILTER_FIELD_NAMES:
        if f in env_vars and env_vars[f] != "":
            filter[f] = env_vars[f]

    # Populate hosts
    if location := env_vars.get('location'):
        bucket_name = locations[location]['bucket_name']
        bucket_type = locations[location]['bucket_type']
        file_path = locations[location]['file_path']
        auth_file = locations[location]['auth_file']
        server = env_vars.get('server')

    # Get servers for each location
    servers = read_toml(SERVERS_FILE)

    splits['get_servers'] = time()

    # Get a list of all files in the bucket
    objects = run(get_objects_list(bucket_name, prefix=file_path, bucket_type=bucket_type, auth_file=auth_file))

    splits['list_objects'] = time()

    requests = {'server': {}, 'client_ip': {}, 'method': {}, 'status_code': {}, 'domain': {}}
    bytes = {'server': {}, 'client_ip': {}, 'domain': {}}

    # Populate list of files to read from bucket
    file_names: dict = {}
    if location:
        servers[location] = []
    for o in objects:
        if o['updated'] < start_time:
            continue
        server = o['name'].split('/')[-1].replace('.log', '')
        servers[location].append(server)
        file_names[server] = o['name']
        requests['server'][server] = 0

    save_toml(SERVERS_FILE, servers)

    splits['save_servers'] = time()

    # Read the log files from the bucket
    blobs = run(read_files_from_bucket(bucket_name, file_names.values(), bucket_type=bucket_type, auth_file=auth_file))
    splits['read_objects'] = time()

    entries = deque()
    for i, server in enumerate(file_names.keys()):
        matches = deque(process_log(blobs[i], time_range, filter, log_fields))
        if i in blobs:
            blobs.remove(i)
        if len(matches) > 0:
            entries.extend(matches)
            requests['server'][server] = len(matches)
    del blobs
    splits['process_objects'] = time()

    # Perform Total Counts
    requests['client_ip'] = Counter([_[2] for _ in entries])
    requests['status_code'] = Counter([_[3] for _ in entries])
    requests['method'] = Counter([_[5] for _ in entries])
    requests['domain'] = Counter([_[6][7:].split("/")[0] if _[6].startswith("http:") else _[6] for _ in entries])
    requests['how'] = Counter([_[8].split("/")[0] for _ in entries])
    for _ in entries:
        bytes['client_ip'][_[2]] = bytes['client_ip'][_[2]] + int(_[4]) if _[2] in bytes['client_ip'] else int(_[4])
    splits['do_counts'] = time()

    # Sort by timestamp reversed, so that latest entries are first in the list
    newest_first: list = sorted(entries, key=lambda x: x[0][:10], reverse=True)
    entries.clear()
    splits['sort_entries'] = time()

    for entry in newest_first:

        # Convert raw unix timestamp to datatime objecet
        timestamp = int(entry[0].split('.')[0])
        time_str = datetime.fromtimestamp(timestamp)
        entry[0] = f"{time_str}"

        # Convert elapsed time from ms to seconds or minutes
        elapsed = int(entry[1])
        unit = "s";
        if elapsed < 1000:
            unit = "ms"
        else:
            elapsed = elapsed / 1000
            if elapsed > 60:
                elapsed = round(elapsed)
        entry[1] = f"{elapsed} {unit}"

        # Convert size from Bytes to KB, MB, TB, or PB
        size = int(entry[4])
        unit = "Bytes"
        if size >= 1000:
            for i, unit in enumerate(UNITS):
                size = round(size / 1000, 3)
                unit = UNITS[i]
                if size < 1000:
                    break
        entry[4] = f"{size} {unit}"

        entries.append(entry)

    entries = [dict(zip(log_fields, entry)) for entry in entries]
    splits['zip_entries'] = time()

    save_toml(STATUS_CODES_FILE, requests['status_code'])
    splits['save_status_codes'] = time()

    if location:
        client_ips = read_toml(CLIENT_IPS_FILE)
        client_ips[location] = list(requests['client_ip'].keys())
        save_toml(CLIENT_IPS_FILE, client_ips)

    splits['save_client_ips'] = time()

    last_split = splits['start']
    durations = {}
    for key, timestamp in splits.items():
        if key != 'start':
            duration = round((splits[key] - last_split), 3)
            durations[key] = f"{duration:.3f}"
            last_split = timestamp
    durations['total'] = f"{round(last_split - splits['start'], 3):.3f}"

    return {
        'entries': list(entries),
        'requests_by_server': requests['server'],
        'requests_by_client_ip': requests['client_ip'],
        'requests_by_method': requests['method'],
        'requests_by_domain': requests['domain'],
        'requests_by_status_code': requests['status_code'],
        'requests_by_how': requests['how'],
        'bytes_by_client_ip': bytes['client_ip'],
        'durations': durations,
        'time_range': time_range,
    }
