from flask import Flask, request, jsonify, render_template, Response
from traceback import format_exc
from main import *

app = Flask(__name__, static_url_path='/static')
app.config['JSON_SORT_KEYS'] = False


@app.route("/")
@app.route("/index.html")
def _root():

    try:
        return render_template('index.html')
    except Exception as e:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/top.html")
def _top():

    try:
        settings = get_settings()
        intervals = settings.get('INTERVALS')
        field_names = settings.get('FIELD_NAMES')
        locations = get_locations_list()
        status_codes = get_status_codes()
        defaults = settings['DEFAULT_VALUES']
        interval = str(defaults.get('interval', 900))

        if location := request.args.get('location', defaults.get('location')):
            servers = get_servers().get(location, [])
            client_ips = get_client_ips().get(location, [])
        else:
            servers = []
            client_ips = []
            status_codes = []
        return render_template(request.path, locations=locations, interval=interval, location=location, servers=servers,
                               client_ips=client_ips, status_codes=status_codes, intervals=intervals)
    except Exception as e:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/middle.html")
def _middle():

    try:
        settings = get_settings()
        server = request.args.get('server')
        client_ip = request.args.get('client_ip')
        env_vars = request.args
        field_names = settings.get('LOG_FIELDS')
        data = get_data(request.args) if 'location' in request.args else dict(entries=[])
        return render_template(request.path, server=server, data=data, num_entries=len(data['entries']),
                               fields=field_names, client_ip=client_ip, env_vars=request.args)
    except Exception as e:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/bottom.html")
def _bottom():

    try:
        return render_template(request.path, locations=get_locations())
    except Exception as e:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/get_data")
def _get_data():

    try:
        settings = get_settings()
        response_headers = settings.get('RESPONSE_HEADERS')
        data = get_data(request.args)
        return jsonify(data), response_headers
    except Exception as e:
        return Response(format_exc(), 500, content_type="text/plain")


if __name__ == '__main__':
    app.run()
