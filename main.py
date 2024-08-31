import os
import pathlib
import tomli
import tomli_w
import asyncio
from collections import deque, Counter
from datetime import datetime
from time import time
from math import floor
from sys import getsizeof
from gcloud.aio.auth import Token
from gcloud.aio.storage import Storage
from google.auth import default
from google.auth.transport.requests import Request

LOG_FIELD_NAMES = ("timestamp", "elapsed", "client_ip", "code", "bytes", "method", "url", "rfc931", "how", "type")
FILTER_FIELD_NAMES = ('client_ip', 'code', 'url')
DEFAULT_FILTER = {}
UNITS = ("KB", "MB", "GB", "TB", "PB")
STORAGE_TIMEOUT = 55
SETTINGS_FILE = 'settings.toml'
LOCATIONS_FILE = 'locations.toml'
SERVERS_FILE = 'servers.toml'
CLIENT_IPS_FILE = 'client_ips.toml'
STATUS_CODES_FILE = 'status_codes.toml'
SCOPES = ["https://www.googleapis.com/auth/cloud-platform.read-only"]


def get_full_path(file_name: str) -> str:

    pwd = os.path.realpath(os.path.dirname(__file__))
    full_path = os.path.join(pwd, file_name)
    _ = pathlib.Path(full_path)
    if not _.is_file():
        raise FileNotFoundError(f"File '{full_path}' does not exist!")
    if not _.stat().st_size > 0:
        raise FileExistsError(f"File '{full_path}' is empty!")
    return full_path


def read_toml(file_name: str) -> dict:

    try:
        if _ := get_full_path(file_name):
            fp = open(_, mode="rb")
            _ = tomli.load(fp)
            fp.close()
            return _
    except FileNotFoundError:
        return {}
    except Exception as e:
        raise e


def write_toml(file_name: str, data: dict):

    try:
        _ = get_full_path(file_name)
        fp = open(_, mode="wb")
        tomli_w.dump(data, fp)
        fp.close()
        return
    except Exception as e:
        raise e


def get_settings() -> dict:

    return read_toml(SETTINGS_FILE)


def get_locations() -> dict:

    return read_toml(LOCATIONS_FILE)


def get_servers(filter: str = None) -> dict:

    return read_toml(SERVERS_FILE)


def get_client_ips(location: str, server_group: str) -> list:

    _ = read_toml(CLIENT_IPS_FILE)
    if location := _.get(location):
        return location.get(server_group, [])
    return []


def object_is_current(obj: dict, min_timestamp: int = 0) -> bool:

    """
    Given a GCS object, return true if size is positive number and updated timestamp is higher than threshold
    """

    if int(obj.get('size', 0)) == 0:
        return False    # Ignore empty files

    timestamp_format = "%Y-%m-%d%H:%M:%S"

    if updated := obj.get('updated'):
        updated_ymd = updated[:10]
        updated_hms = updated[11:19]
        updated_timestamp = int(datetime.timestamp(datetime.strptime(updated_ymd + updated_hms, timestamp_format)))
        if updated_timestamp > min_timestamp:
            return True

    return False


async def list_storage_objects(bucket: str, token: Token, prefix: str = "", time_range: tuple = None) -> list:

    """
    Given a GCS bucket and prefix, return all non-zero byte objects within the specified time range
    """

    default_time_range = (0, time())
    time_range = time_range if time_range and len(time_range) == 2 else default_time_range
    params = {'prefix': prefix}
    objects = []

    try:
        async with Storage(token=token) as storage:
            while True:
                _ = await storage.list_objects(bucket, params=params, timeout=STORAGE_TIMEOUT)
                objects.extend(_.get('items', []))
                if next_page_token := _.get('nextPageToken'):
                    params.update({'pageToken': next_page_token})
                else:
                    break
    except Exception as e:
        raise e

    return [o for o in objects if object_is_current(o, time_range[0])]


async def get_storage_objects(bucket: str, token: Token, objects: list = None) -> deque:

    """
    Given a GCS bucket name and list of files, return the contents of the files
    """

    if objects is None:
        file_names = []
    try:
        async with Storage(token=token) as storage:
            tasks = (storage.download(bucket, o, timeout=STORAGE_TIMEOUT) for o in objects)
            _ = deque(await asyncio.gather(*tasks))
        await token.close()
        return _
    except Exception as e:
        raise e


async def process_log(blob: bytes, time_range: tuple, log_filter: dict = None, log_fields: dict = None) -> deque:

    matches = deque()

    lines = deque(blob.decode('utf-8').rstrip().splitlines())
    del blob

    while len(lines) > 0:

        # Work backwards on file, since newer entries are at the end
        line = tuple(lines.pop().split())
        entry = dict(zip(log_fields.values(), line))

        if len(line) < len(log_fields):
            continue
        if entry['code'] == "NONE/000":
            continue

        # Check if timestamp is within search range
        timestamp = int(entry.get('timestamp')[0:10])
        if timestamp >= time_range[1]:
            continue  # haven't read enough
        if timestamp <= time_range[0]:
            break  # read too far

        match = True
        if log_filter:
            match = False
            for k, v in log_filter.items():
                if v in entry.get(k):
                    match = True
                    break
        if not match:
            continue

        time_str = str(datetime.fromtimestamp(timestamp))
        entry.update({'timestamp': time_str})

        if elapsed := int(entry['elapsed']):
            unit = "s"
            if elapsed < 1000:
                unit = "ms"
            else:
                elapsed = elapsed / 1000
                if elapsed > 60:
                    elapsed = round(elapsed)
            elapsed_str = f"{elapsed} {unit}"
        else:
            elapsed_str = "unknown"
        entry.update({'elapsed': elapsed_str})

        if size := int(entry['bytes']):
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
        entry.update({'size': size_str})

        if url := entry['url']:
            if url.startswith("http:"):
                # Remove full URL from HTTP requests
                host = url[7:].split('/')[0]
                host = host if ":" in host else f"{host}:80"
            else:
                host = url
        else:
            host = "unknownhost.unknowndomain"
        entry.update({'host': host})

        matches.append(entry)

    return matches


async def get_data(env_vars: dict = None) -> dict:

    splits = {'start': time()}

    settings = get_settings()
    default_values = settings.get('DEFAULT_VALUES', {})
    assert settings, "Could not load settings.  Does {} exist?".format(SETTINGS_FILE)
    if not (log_fields := settings.get('LOG_FIELDS')):
        log_fields = dict(enumerate(LOG_FIELD_NAMES))
    splits['get_settings'] = time()

    locations = get_locations()
    assert locations, "Could not load locations.  Does {} exist?".format(LOCATIONS_FILE)
    splits['get_locations'] = time()

    #action = env_vars.get('action')
    #if action == "get_locations":
    #    return {'locations': locations.keys()}

    #location = env_vars.get('location')
    #if action == "get_servers":
    #    servers = get_servers()
    #    if location in servers:
    #        return {'servers': locations.keys()}

    now = time()
    # Parse parameters to determine time range
    interval = int(env_vars.get('interval', default_values.get('interval', 7200)))
    if env_vars.get('end_time', "") != "":
        end_time = int(env_vars['end_time'])
    else:
        end_time = floor(now)
    start_time = int(env_vars.get('start_time', end_time - interval))
    time_range = (start_time, end_time)

    # Parse parameters to determine filter
    filter = {
        'code': env_vars.get('status_code', ""),
        #'client_ip': env_vars.get('client_ip', ""),
    }

    # Populate variables
    if not (location := env_vars.get('location')):
        location = default_values.get('location', list(locations.keys())[0])
    _ = locations[location]
    bucket_name = _.get('bucket_name')
    bucket_type = _.get('bucket_type')
    file_path = _.get('file_path')
    auth_file = _.get('auth_file')
    server_group = env_vars.get('server_group')
    servers = {location: []}

    # Get servers for each location
    servers_cache = read_toml(SERVERS_FILE)

    splits['get_servers'] = time()

    try:
        token = None
        credentials = None
        if auth_file:
            _ = get_full_path(auth_file)
            token = Token(service_file=_, scopes=SCOPES)
        else:
            credentials, project_id = default(scopes=SCOPES)
            _ = Request()
            credentials.refresh(_)
            token = credentials.token
    except Exception as e:
        raise e
    splits['get_token'] = time()

    objects = await list_storage_objects(bucket_name, token=token, prefix=file_path, time_range=time_range)
    splits['list_objects'] = time()

    request_counts = {k: {} for k in ['server', 'client_ip', 'method', 'status_code', 'domain']}

    # Populate list of files to read from bucket
    file_names = {}
    #for o in objects:
    while len(objects) > 0:
        o = objects.pop()
        if 'squid_parse_output' in o['name']:
            continue
        server_name = o['name'].split('/')[-1].replace('.log', '')
        match = True
        if server_group and server_group != "" and server_group != "all":
            if server_group not in server_name:
                match = False
        if match:
            servers[location].append(server_name)
            file_names.update({server_name: o['name']})
            request_counts['server'].update({server_name: 0})
    splits['filter_objects'] = time()

    #if action == "get_servers":
    #    write_toml(SERVERS_FILE, servers)
    #    return list(servers[location])
    #splits['save_servers'] = time()

    # Read the objects from the bucket
    blobs = await get_storage_objects(bucket_name, token, list(file_names.values()))
    splits['read_objects'] = time()

    byte_counts = {k: {} for k in ['server', 'client_ip', 'domain']}

    entries = deque()
    for i, server in enumerate(file_names.keys()):
        matches = await process_log(blobs.popleft(), time_range, filter, log_fields)
        # Inject the server name as a new field
        #print(server)
        #matches = [match.update({'server': server}) for match in matches]
        entries.extend(matches)
        request_counts['server'].update({server: len(matches)})
        byte_counts['server'].update({server: 69})
    splits['process_objects'] = time()
    matches = []

    # Perform Total Counts
    for field in ['client_ip', 'code', 'method']:
        request_counts.update({field: Counter([_.get(field) for _ in entries])})

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
        if location not in client_ips:
            client_ips[location] = {}
        client_ips[location][server_group] = list(request_counts['client_ip'].keys())
        write_toml(CLIENT_IPS_FILE, client_ips)
    splits['save_client_ips'] = time()

    last_split = splits['start']
    durations = {}
    for key, timestamp in splits.items():
        if key != 'start':
            duration = round((splits[key] - last_split), 3)
            durations[key] = f"{duration:.3f}"
            last_split = timestamp
    durations['total'] = f"{round(last_split - splits['start'], 3):.3f}"

    sizes = {
        'objects': getsizeof(objects),
        'blobs': sum([getsizeof(o) for o in blobs]),
        'entries': getsizeof(entries),
        'request_counts': getsizeof(request_counts),
        'matches': sum([getsizeof(o) for o in matches]),
    }
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
        'sizes': sizes,
        'time_range': time_range,
        #'num_servers': len(servers[location]),
    }


if __name__ == '__main__':

    try:
        arguments = {}
        _ = asyncio.run(get_data(arguments))
        print(_['durations'], "\n", _['sizes'])
    except Exception as e:
        quit(e)
