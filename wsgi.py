from random import randint
from traceback import format_exc
from asyncio import run
from flask import Flask, request, jsonify, render_template, Response, session
from main import get_settings, get_locations, get_data, get_client_ips, read_cache_file, write_cache_file


DEFAULTS = {
    'response_headers': {'Cache-Control': "no-cache, no-store"},
    'server_groups':  {'all': "All Servers"},
    'content_type': "text/plain",
}

app = Flask(__name__, static_url_path='/static')
app.config['JSON_SORT_KEYS'] = False
app.config['SESSION_COOKIE_SAMESITE'] = "Strict"
app.secret_key = str(randint(0, 1000000))


@app.route("/")
@app.route("/index.html")
def _root():

    try:
        return render_template('index.html')
    except Exception as e:
        return Response(format_exc(), 500, content_type=DEFAULTS['content_type'])


@app.route("/top.html")
def _top():

    try:
        settings = get_settings()
        intervals = settings.get('INTERVALS')
        locations = get_locations()
        defaults = settings['DEFAULT_VALUES']

        values = {}
        for field_name in ('location', 'server_group', 'interval', 'status_code'):
            # If value specified in query string, use that
            if field_value := request.args.get(field_name):
                print("request arg:", field_name, "is set to", field_value)
                #if session.get(field_name) != field_value:
                #    session[field_name] = field_value  # Set or update cookie
            else:
                # Try to fetch from cookie, if not, use defaults
                if not (field_value := session.get(field_name)):
                    field_value = str(defaults.get(field_name, ""))
            values[field_name] = field_value
            print('values:', values)

        server_groups = []
        client_ips = []
        status_codes = []
        if location := values.get('location'):
            server_groups = locations[location].get('server_groups', DEFAULTS['server_groups'])
            if server_group := values.get('server_group'):
                client_ips = get_client_ips(location, server_group)
                print("Got client", len(client_ips), "IPs for", location, server_group)
            _ = read_cache_file('status_codes')
            status_codes = _.get(location, [])
        return render_template(request.path, locations=locations, interval=values['interval'], location=location,
                               server_groups=server_groups, server_group=values['server_group'], client_ips=client_ips,
                               status_codes=status_codes, intervals=intervals, status_code=values['status_code'])
    except Exception as e:
        return Response(format_exc(), 500, content_type=DEFAULTS['content_type'])


@app.route("/middle.html")
def _middle():

    try:
        settings = get_settings()
        if _server_group := request.args.get('server_group'):
            session['server_group'] = _server_group  # Set a Cookie
        else:
            _server_group = session.get('server_group')
        _client_ip = request.args.get('client_ip')
        _fields = {'-1': 'server_name'}
        _ = settings.get('LOG_FIELDS')
        _fields.update(_)
        _data = run(get_data(request.args)) if 'location' in request.args else dict(entries=[])
        _num_entries = len(_data['entries'])
        return render_template(request.path, server_group=_server_group, data=_data,
                               num_entries=_num_entries,
                               fields=_fields, client_ip=_client_ip, env_vars=request.args)
    except Exception as e:
        return Response(format_exc(), 500, content_type=DEFAULTS['content_type'])


@app.route("/bottom.html")
def _bottom():

    try:
        locations = get_locations()
        return render_template(request.path, locations=locations)
    except Exception as e:
        return Response(format_exc(), 500, content_type=DEFAULTS['content_type'])


@app.route("/get_data")
def _get_data():

    try:
        settings = get_settings()
        response_headers = settings.get('RESPONSE_HEADERS', DEFAULTS['response_headers'])
        data = run(get_data(request.args)) if request.args.get('location') else dict(entries=[])
        return jsonify(data), response_headers
    except Exception as e:
        return Response(format_exc(), 500, content_type=DEFAULTS['content_type'])


if __name__ == '__main__':

    app.run(debug=True)
