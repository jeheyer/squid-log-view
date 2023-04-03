from os import path
from tomli import load
from tomli_w import dump

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


def get_locations() -> dict:

    return read_toml(LOCATIONS_FILE)


def get_servers() -> dict:

    return read_toml(SERVERS_FILE)


def get_locations_list() -> list:

    return list(get_locations().keys())


def get_client_ips() -> dict:

    try:
        _ = read_toml(CLIENT_IPS_FILE)
        if _:
            return _
    except Exception as e:
        raise e

    return {}


def get_status_codes() -> list:

    return read_toml(STATUS_CODES_FILE).keys()
