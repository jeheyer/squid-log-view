from os.path import realpath, dirname, join, exists, getsize
from asyncio import gather, run
from time import time
from math import floor
from sys import getsizeof
from collections import deque, Counter
import tomli
import json
from datetime import datetime
from tempfile import gettempdir
from gcloud.aio.auth import Token
from gcloud.aio.storage import Storage
from google.auth.transport.requests import Request

LOG_FIELD_NAMES = ("timestamp", "elapsed", "client_ip", "status_code", "bytes", "method", "url", "rfc931", "how", "type")
FILTER_FIELD_NAMES = ('client_ip', 'status_code', 'url')
DEFAULT_STATUS_CODES = ("TCP_TUNNEL/200", "TCP_MEM_HIT/200", "TCP_REFRESH_MODIFIED/200", "TCP_MISS/304", "TCP_REFRESH_UNMODIFIED/304", "NONE_NONE/503")
IGNORE_STATUS_CODES = ('NONE/000')
DEFAULT_FILTER = {}
UNITS = ("KB", "MB", "GB", "TB", "PB")
STORAGE_TIMEOUT = 55
REQUEST_COUNT_FIELDS = ('server', 'client_ip', 'method', 'status_code', 'domain')
SETTINGS_FILE = 'settings.toml'
LOCATIONS_FILE = 'locations.toml'
TEMPDIR = gettempdir()
SCOPES = ["https://www.googleapis.com/auth/cloud-platform.read-only"]


def ping():

    try:
        return get_settings()
    except Exception as e:
        return e


def get_full_path(file_name: str) -> str:

    pwd = realpath(dirname(__file__))
    full_path = join(pwd, file_name)
    if not exists(full_path):
        raise FileNotFoundError(f"File '{full_path}' does not exist!")
    if getsize(full_path) < 1:
        raise FileExistsError(f"File '{full_path}' is empty!")
    return full_path


def read_cache_file(data_type: str) -> dict:

    try:
        cache_file = data_type + ".json"
        if cache_file := join(TEMPDIR, cache_file):
            fp = open(cache_file, mode="rb")
            _ = json.load(fp)
            fp.close()
            #print(f"Successfully read cache file for '{data_type}': {cache_file}")
            return _
    except FileNotFoundError:
        return {}
    except Exception as e:
        raise e


def write_cache_file(data_type: str, data: dict) -> bool:

    try:
        cache_file = data_type + ".json"
        if cache_file := join(TEMPDIR, cache_file):
            fp = open(cache_file, mode="w")
            json.dump(data, fp)
            fp.close()
            #print(f"Successfully wrote cache file for '{data_type}': {cache_file}")
            return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise e


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


def get_settings() -> dict:

    return read_toml(SETTINGS_FILE)


def get_locations() -> dict:

    return read_toml(LOCATIONS_FILE)


def get_servers(filter: str = None) -> dict:

    _ = read_cache_file('servers')
    return _


def get_client_ips(location: str, server_group: str) -> list:

    _ = read_cache_file('client_ips')
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


async def get_storage_objects(bucket: str, token: Token, objects: list = ()) -> deque:

    """
    Given a GCS bucket name and list of files, return the contents of the files
    """
    try:
        async with Storage(token=token) as storage:
            tasks = (storage.download(bucket, o, timeout=STORAGE_TIMEOUT) for o in objects)
            _ = deque(await gather(*tasks))
        await token.close()
        return _
    except Exception as e:
        raise e


async def process_log(server_name: str, blob: bytes, time_range: tuple, log_filter: dict = None, log_fields: dict = None) -> deque:

    matches = deque()

    lines = deque(blob.decode('utf-8').rstrip().splitlines())
    del blob

    while len(lines) > 0:

        # Work backwards on file, since newer entries are at the end
        line = tuple(lines.pop().split())
        entry = {'server_name': server_name}
        entry.update({v: line[int(k)] for k, v in log_fields.items()})

        # Skip lines that have invalid / unexpected data
        if len(line) < len(log_fields):
            continue
        # Skip lines that have special codes
        if entry['status_code'] in IGNORE_STATUS_CODES:
            continue

        # Check if timestamp is within search range
        timestamp = int(entry.get('timestamp')[0:10])
        if timestamp >= time_range[1]:
            continue  # haven't read enough, so skip this line
        if timestamp <= time_range[0]:
            break  # read too far, so break

        if all(not v or v == "" for v in log_filter.values()):
            match = True
        else:
            match = False
            #print("log_filter:", log_filter)
            for k, v in log_filter.items():
                if not v or v == "":
                    continue
                if v in entry.get(k):
                    match = True
                else:
                    match = False
                    break
        if not match:
            continue  # Skip entry if filter specified, but no match

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

        if url := entry.get('url'):
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
    #if not (log_fields := settings.get('LOG_FIELDS')):
    log_fields = dict(enumerate(LOG_FIELD_NAMES))
    splits['get_settings'] = time()

    locations = get_locations()
    assert locations, f"Could not load locations.  Does {LOCATIONS_FILE} exist?"
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
    filter = {k: env_vars.get(k, "") for k in FILTER_FIELD_NAMES}

    # Populate variables
    if not (location := env_vars.get('location')):
        location = default_values.get('location', list(locations.keys())[0])
    assert locations.get(location), f"Could not find location '{location}' in locations list"
    _ = locations[location]
    bucket_name = _.get('bucket_name')
    bucket_type = _.get('bucket_type')
    file_path = _.get('file_path')
    auth_file = _.get('auth_file')
    server_group = env_vars.get('server_group')
    servers = {location: []}

    # Get servers for each location
    #servers_cache = read_toml(SERVERS_FILE)
    #servers = read_cache_file('servers')

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

    request_counts = {k: {} for k in REQUEST_COUNT_FIELDS}

    # Populate list of files to read from bucket
    file_names = {}
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

    # Read the objects from the bucket
    server_names = list(file_names.keys())
    object_names = list(file_names.values())
    blobs = await get_storage_objects(bucket_name, token, object_names)
    splits['read_objects'] = time()

    byte_counts = {k: {} for k in ['server', 'client_ip', 'domain']}

    entries = deque()
    for i, server_name in enumerate(server_names):
        _blob = blobs.popleft()
        matches = await process_log(server_name, _blob, time_range, filter, log_fields)
        # Inject the server name as a new field
        #print(server)
        #matches = [match.update({'server': server}) for match in matches]
        entries.extend(matches)
        request_counts['server'].update({server_name: len(matches)})
        byte_counts['server'].update({server_name: 69})
    splits['process_objects'] = time()
    matches = []

    # Perform Total Counts
    for field in ('client_ip', 'status_code', 'method'):
        request_counts[field] = Counter([_.get(field) for _ in entries])

    request_counts.update({
        'domain': Counter([_['host'][7:].split("/")[0] if _['host'].startswith("http:") else _['host'] for _ in entries]),
        'how':  Counter([_['how'].split("/")[0] for _ in entries]),
    })
    for _ in entries:
        _client_ip = _['client_ip']
        _bytes = int(_['bytes'])
        byte_counts['client_ip'][_client_ip] = byte_counts['client_ip'].get(_client_ip, 0) + _bytes
    splits['do_counts'] = time()

    # Sort by timestamp reversed, so that latest entries are first in the list
    entries = sorted(entries, key=lambda x: x['timestamp'], reverse=True)
    splits['sort_entries'] = time()

    status_codes = read_cache_file('status_codes')
    if location not in status_codes:
        #status_codes[location] = list(DEFAULT_STATUS_CODES)
        status_codes[location] = list(default_values.get('STATUS_CODES', DEFAULT_STATUS_CODES))
    #print("request counts:", request_counts)
    #print("status_codes:", status_codes)

    status_codes[location] = list(set(status_codes[location] + list(request_counts['status_code'].keys())))
    #print("status_codes:", status_codes)
    _ = write_cache_file('status_codes', status_codes)
    splits['save_status_codes'] = time()

    if server_group:
        # Update Client IPs Cache
        client_ips = read_cache_file('client_ips')
        if location not in client_ips:
            client_ips[location] = {server_group: []}
        client_ips[location][server_group] = list(request_counts['client_ip'].keys())
        write_cache_file('client_ips', client_ips)
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
        _ = run(get_data(arguments))
        print(_['durations'], "\n", _['sizes'])
    except Exception as e:
        quit(e)
