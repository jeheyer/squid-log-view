from quart import Quart, request, render_template, Response, jsonify
from traceback import format_exc
from asyncio import run
from main import *

app = Quart(__name__, static_url_path='/static')
app.config['JSON_SORT_KEYS'] = False

INTERVALS = {
    '120': "Last 2 Minutes",
    '300': "Last 5 Minutes",
    '600': "Last 10 Minutes",
    '1800': "Last 30 Minutes",
    '3600': "Last 1 Hour",
    '1440': "Last 4 Hours",
    '86400': "Last 1 Day",
}
DEFAULT_INTERVAL = 1800

RESPONSE_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'no-cache, no-store',
    'Pragma': 'no-cache'
}


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
        location = request.args.get('location')
        interval = request.args.get('interval')
        locations = get_locations_list()
        if location:
            servers = get_servers().get(location, [])
            client_ips = get_client_ips().get(location, [])
            status_codes = get_status_codes()
        else:
            servers = []
            client_ips = []
        return run(render_template('top.html', locations=locations, interval=interval, location=location,
            servers=servers, client_ips=client_ips, status_codes=status_codes, intervals=INTERVALS))
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


@app.route("/middle.html")
def _middle():

    try:
        data = get_data(request.args)
        return run(render_template('middle.html', data=data, num_entries=len(data['entries']),
                                   fields=list(data['entries'][0].keys())))
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


@app.route("/bottom.html")
def _bottom():

    try:
        return run(render_template('bottom.html', locations=get_locations()))
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


@app.route("/get_data")
def _get_data():

    try:
        data = get_data(request.args)
        return jsonify(data), RESPONSE_HEADERS
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


if __name__ == '__main__':
    app.run()

