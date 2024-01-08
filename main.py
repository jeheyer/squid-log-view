from asyncio import run, gather
from collections import deque, Counter
from time import time
from math import floor
from os import path
from tomli import load
from tomli_w import dump
from gcloud.aio.auth import Token
from gcloud.aio.storage import Storage
from datetime import datetime

FILTER_FIELD_NAMES: tuple = ('client_ip', 'code', 'url')
DEFAULT_FILTER: dict = {}
UNITS: tuple = ("KB", "MB", "GB", "TB", "PB")
STORAGE_TIMEOUT = 60
SETTINGS_FILE = 'settings.toml'
LOCATIONS_FILE = 'locations.toml'
SERVERS_FILE = 'servers.toml'
CLIENT_IPS_FILE = 'client_ips.toml'
STATUS_CODES_FILE = 'status_codes.toml'


def read_toml(file_name: str) -> dict:

    try:
        pwd = path.realpath(path.dirname(__file__))
        if path.isfile(locations_file := path.join(pwd, file_name)):
            fp = open(locations_file, mode="rb")
            return load(fp)
        else:
            return {}
    except Exception as e:
        raise e


def save_toml(file_name: str, data: dict):

    try:
        with open(file_name, mode="wb") as fp:
            dump(data, fp)
    except Exception as e:
        raise e


def get_settings() -> dict:

    return read_toml(SETTINGS_FILE)


def get_locations() -> dict:

    return read_toml(LOCATIONS_FILE)


def get_servers(filter: str = None) -> dict:

    return read_toml(SERVERS_FILE)


def get_client_ips(location: str, server_group: str) -> list:

    try:
        _ = read_toml(CLIENT_IPS_FILE)
        if location := _.get(location):
            return location.get(server_group, [])
    except Exception as e:
        raise e

    return []


def object_is_current(obj: dict, time_range) -> bool:

    """
    Given a GCS object, return true if size is positive number and updated timestamp is higher than threshold
    """

    if int(obj.get('size', 0)) == 0:
        return False    # Ignore empty files

    if updated := obj.get('updated'):
        updated_ymd = updated[:10]
        updated_hms = updated[11:19]
        updated_timestamp = int(datetime.timestamp(datetime.strptime(updated_ymd + updated_hms, "%Y-%m-%d%H:%M:%S")))
        if updated_timestamp > time_range[0]:
            return True

    return False


async def list_storage_objects(bucket_name: str, token: Token, prefix: str = "", time_range: tuple = None) -> tuple:

    """
    Given a GCS bucket and prefix, return all non-zero byte objects within the specified time range
    """

    time_range = time_range if time_range and len(time_range) == 2 else (0, time())
    params = {'prefix': prefix}
    objects = []

    try:
        async with Storage(token=token) as storage:
            while True:
                _ = await storage.list_objects(bucket_name, params=params, timeout=STORAGE_TIMEOUT)
                objects.extend(_.get('items', []))
                if next_page_token := _.get('nextPageToken'):
                    params.update({'pageToken': next_page_token})
                else:
                    break
    except Exception as e:
        raise e

    return tuple([o for o in objects if object_is_current(o, time_range)])


async def get_storage_objects(bucket_name: str, token: Token, file_names: list = []) -> tuple:

    """
    Given a GCS bucket name and list of files, return the contents of the files
    """

    try:
        async with Storage(token=token) as storage:
            tasks = (storage.download(bucket_name, file_name, timeout=STORAGE_TIMEOUT) for file_name in file_names)
            blobs = tuple(await gather(*tasks))
        await token.close()
        return blobs
    except Exception as e:
        raise e
        return tuple("")


async def process_log(blob: str, time_range: tuple, filter: dict = {}, log_fields={}) -> deque():

    matches = deque()

    try:

        if filter:
            filter_indexes = {int(i): field_name for i, field_name in log_fields.items() if field_name in filter}

        lines = deque(blob.decode('utf-8').splitlines())
        while len(lines) > 0:
            # Work backwards on file, since newer entries are at the end
            line = lines.pop()
            _ = tuple(line.split())
            if len(_) < len(log_fields):
                continue
            if _[3] == "NONE/000":
                continue

            timestamp = int(_[0][0:10])
            if timestamp > time_range[1]:
                continue  # haven't read enough
            if timestamp < time_range[0]:
                break  # read too far
            """
            if entry[6].startswith("http:"):
                # Remove full URL from HTTP requests
                host = entry[6][7:].split('/')[0]
                if ":" in host:
                    entry[6] = host
                else:
                    entry[6] = host + ":80"
            """
            match = False
            if filter:
                for i, field_name in filter_indexes.items():
                    match = True if filter.get(field_name) in _[i] else False
            else:
                match = True
            if match:
                time_str = str(datetime.fromtimestamp(timestamp))
                if elapsed := int(_[1]):
                    unit = "s";
                    if elapsed < 1000:
                        unit = "ms"
                    else:
                        elapsed = elapsed / 1000
                        if elapsed > 60:
                            elapsed = round(elapsed)
                    elapsed_str = f"{elapsed} {unit}"
                else:
                    elapsed_str = "unknown"
                if size := int(_[4]):
                    unit = "Bytes"
                    if size >= 1000:
                        for i, unit in enumerate(UNITS):
                            size = round(size / 1000, 3)
                            unit = UNITS[i]
                            if size < 1000:
                                break
                    size_str = f"{size} {unit}"
                else:
                    size_str = "unknown"
                if url := _[6]:
                    if url.startswith("http:"):
                        # Remove full URL from HTTP requests
                        host = url[7:].split('/')[0]
                        host = host if ":" in host else f"{host}:80"
                    else:
                        host = url
                else:
                    host = "unknownhost.unknowndomain"
                matches.append({
                    'timestamp': time_str,
                    'elapsed': elapsed_str,
                    'client_ip': _[2],
                    'code': _[3],
                    'bytes': _[4],
                    'size': size_str,
                    'method': _[5],
                    'host': host,
                    'rfc931': _[7],
                    'how': _[8],
                    'type': _[9],
                })

    except Exception as e:
        raise e

    return matches


async def get_data(env_vars: dict = {}) -> dict:

    splits = {'start': time()}

    settings = get_settings()
    assert settings, "Could not load settings.  Does {} exist?".format(SETTINGS_FILE)
    log_fields = settings.get('LOG_FIELDS')
    splits['get_settings'] = time()

    try:
        locations = get_locations()
    except Exception as e:
        raise e
    assert locations, "Could not load locations.  Does {} exist?".format(LOCATIONS_FILE)
    splits['get_locations'] = time()

    # Process parameters
    location = env_vars.get('location')
    action = env_vars.get('action')
    if action == "get_locations":
        return list(locations.keys())
    if action == "get_servers":
        servers = get_servers()
        if location in servers:
            return list(servers[location])

    now = time()
    # Parse parameters to determine time range
    interval = int(env_vars.get('interval', settings['DEFAULT_VALUES'].get('interval', 900)))
    if env_vars.get('end_time', "") != "":
        end_time = int(env_vars['end_time'])
    else:
        end_time = floor(now)
    start_time = int(env_vars.get('start_time', end_time - interval))
    time_range = (start_time, end_time)

    # Parse parameters to determine filter
    filter = {
        'code': env_vars.get('status_code', ""),
        'client_ip': env_vars.get('client_ip', ""),
    }
    #for f in FILTER_FIELD_NAMES:
    #    if f in env_vars and env_vars[f] != "":
    #        filter[f] = env_vars[f]

    # Populate hosts
    if location := env_vars.get('location'):
        bucket_name = locations[location]['bucket_name']
        bucket_type = locations[location]['bucket_type']
        file_path = locations[location]['file_path']
        auth_file = locations[location]['auth_file']
        server_group = env_vars.get('server_group')
        servers = {location: []}

    # Get servers for each location
    try:
        servers_cache = read_toml(SERVERS_FILE)
    except Exception as e:
        return e

    splits['get_servers'] = time()

    try:
        pwd = path.realpath(path.dirname(__file__))
        service_file = path.join(pwd, auth_file)
        token = Token(service_file=service_file, scopes=["https://www.googleapis.com/auth/cloud-platform.read-only"])
    except Exception as e:
        raise e
    splits['get_token'] = time()

    objects = await list_storage_objects(bucket_name, token=token, prefix=file_path, time_range=time_range)
    splits['list_objects'] = time()

    request_counts = {k: {} for k in ['server', 'client_ip', 'method', 'status_code', 'domain']}

    # Populate list of files to read from bucket
    file_names = {}

    for o in objects:
        if 'squid_parse_output' in o['name']:
            continue
        server_name = o['name'].split('/')[-1].replace('.log', '')
        match = True
        if server_group and server_group != "" and server_group != "all":
            if not server_group in server_name:
                match = False
        if match:
            servers[location].append(server_name)
            file_names.update({server_name: o['name']})
            request_counts['server'].update({server_name: 0})
    splits['filter_objects'] = time()

    if action == "get_servers":
        save_toml(SERVERS_FILE, servers)
        return list(servers[location])
    splits['save_servers'] = time()

    # Read the objects from the bucket
    blobs = await get_storage_objects(bucket_name, token, file_names.values())
    splits['read_objects'] = time()

    byte_counts = {k: {} for k in ['server', 'client_ip', 'domain']}

    entries = deque()
    for i, server in enumerate(file_names.keys()):
        matches = await process_log(blobs[i], time_range, filter, log_fields)
        request_counts['server'].update({server: len(matches)})
        byte_counts['server'].update({server: 69})
        entries.extend(matches)
    del blobs
    splits['process_objects'] = time()

    # Perform Total Counts
    for field in ['client_ip', 'code', 'method']:
        request_counts.update({field: Counter([_[field] for _ in entries])})
    request_counts.update({
        'domain': Counter([_['host'][7:].split("/")[0] if _['host'].startswith("http:") else _['host'] for _ in entries]),
        'how':  Counter([_['how'].split("/")[0] for _ in entries]),
    })
    for _ in entries:
        client_ip = _['client_ip']
        bytes = int(_['bytes'])
        byte_counts['client_ip'][client_ip] = byte_counts['client_ip'][client_ip] + bytes if byte_counts['client_ip'].get(client_ip) else bytes
    splits['do_counts'] = time()

    # Sort by timestamp reversed, so that latest entries are first in the list
    entries = sorted(entries, key=lambda x: x['timestamp'], reverse=True)
    splits['sort_entries'] = time()

    if location and server_group:
        client_ips = read_toml(CLIENT_IPS_FILE)
        if not location in client_ips:
            client_ips[location] = {}
        client_ips[location][server_group] = list(request_counts['client_ip'].keys())
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
        'entries': entries,
        'filter': filter,
        'requests_by_server': request_counts['server'],
        'requests_by_client_ip': request_counts['client_ip'],
        'requests_by_method': request_counts['method'],
        'requests_by_domain': request_counts['domain'],
        'requests_by_status_code': request_counts['status_code'],
        'requests_by_how': request_counts['how'],
        'bytes_by_client_ip': byte_counts['client_ip'],
        'durations': durations,
        'time_range': time_range,
        #'num_servers': len(servers[location]),
    }


if __name__ == '__main__':

    locations = get_locations()
    _ = run(get_data({'location': list(locations.keys())[0]}))
    print(_['durations'])
