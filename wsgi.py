from flask import Flask, request, jsonify, render_template, Response, session
from traceback import format_exc
from random import randint
from main import *

DEFAULT_RESPONSE_HEADERS = {'Cache-Control': "no-cache, no-store"}
DEFAULT_SERVER_GROUPS = {'all': "All Servers"}
DEFAULT_STATUS_CODES = ["200", "400", "301", "403", "302", "500", "502", "503"]
PLAIN_TEXT_CONTENT_TYPE = "text/plain"


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
        return Response(format_exc(), 500, content_type=PLAIN_TEXT_CONTENT_TYPE)


@app.route("/top.html")
def _top():
    try:
        settings = get_settings()
        intervals = settings.get('INTERVALS')
        locations = get_locations()
        defaults = settings['DEFAULT_VALUES']

        values = {}
        for field_name in ['location', 'server_group', 'interval', 'status_code']:
            # If value specified in query string, use that
            if field_value := request.args.get(field_name):
                if session.get(field_name) != field_value:
                    session[field_name] = field_value  # Set or update cookie
            else:
                # Try to fetch from cookie, if not, use defaults
                if not(field_value := session.get(field_name)):
                    field_value = str(defaults.get(field_name, ""))
            values[field_name] = field_value

        server_groups = []
        client_ips = []
        if location := values.get('location'):
            server_groups = locations[location].get('server_groups', DEFAULT_SERVER_GROUPS)
            if server_group := values.get('server_group'):
                client_ips = get_client_ips(location, server_group)

        status_codes = settings.get('DEFAULT_VALUES', {}).get('STATUS_CODES', DEFAULT_STATUS_CODES)

        return render_template(request.path, locations=locations, interval=values['interval'], location=location,
                               server_groups=server_groups, server_group=values['server_group'], client_ips=client_ips,
                               status_codes=status_codes, intervals=intervals, status_code=values['status_code'])
    except Exception as e:
        return Response(format_exc(), 500, content_type=PLAIN_TEXT_CONTENT_TYPE)


@app.route("/middle.html")
def _middle():
    try:
        settings = get_settings()
        if server_group := request.args.get('server_group'):
            session['server_group'] = server_group
        else:
            server_group = session.get('server_group')
        client_ip = request.args.get('client_ip')
        field_names = settings.get('LOG_FIELDS')
        data = get_data(request.args) if 'location' in request.args else dict(entries=[])
        return render_template(request.path, server_group=server_group, data=data,
                               num_entries=len(data['entries']),
                               fields=field_names, client_ip=client_ip, env_vars=request.args)
    except Exception as e:
        return Response(format_exc(), 500, content_type=PLAIN_TEXT_CONTENT_TYPE)


@app.route("/bottom.html")
def _bottom():
    try:
        return render_template(request.path, locations=get_locations())
    except Exception as e:
        return Response(format_exc(), 500, content_type=PLAIN_TEXT_CONTENT_TYPE)


@app.route("/get_data")
def _get_data():
    try:
        settings = get_settings()
        response_headers = settings.get('RESPONSE_HEADERS', DEFAULT_RESPONSE_HEADERS)
        data = get_data(request.args)
        return jsonify(data), response_headers
    except Exception as e:
        return Response(format_exc(), 500, content_type=PLAIN_TEXT_CONTENT_TYPE)


if __name__ == '__main__':
    app.run()
