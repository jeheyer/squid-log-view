from quart import Quart, request, render_template, Response, jsonify
from traceback import format_exc
from asyncio import run
from main import *

app = Quart(__name__, static_url_path='/static')
#app.config['JSON_SORT_KEYS'] = Falses


@app.route("/")
@app.route("/index.html")
def _root():
    try:
        return run(render_template('index.html'))
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


@app.route("/top.html")
def _top():

    try:
        settings = get_settings()
        intervals = settings.get('INTERVALS')
        field_names = settings.get('FIELD_NAMES')
        locations = get_locations_list()
        location = request.args.get('location')
        interval = request.args.get('interval')

        if location:
            servers = get_servers().get(location, [])
            client_ips = get_client_ips().get(location, [])
            status_codes = get_status_codes()
        else:
            servers = []
            client_ips = []
        return run(render_template('top.html', locations=locations, interval=interval, location=location,
            servers=servers, client_ips=client_ips, status_codes=status_codes, intervals=intervals))
    except Exception as e:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/middle.html")
def _middle():

    try:
        settings = get_settings()
        field_names = settings.get('LOG_FIELDS')
        data = get_data(request.args)
        return run(render_template('middle.html', data=data, num_entries=len(data['entries']),
                                   fields=field_names))
    except Exception as e:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/bottom.html")
def _bottom():

    try:
        return run(render_template('bottom.html', locations=get_locations()))
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

