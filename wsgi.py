from flask import Flask, request, jsonify, render_template, Response
from main import *
from traceback import format_exc

app = Flask(__name__, static_url_path='/static')
app.config['JSON_SORT_KEYS'] = False

INTERVALS = {
    '120': "Last 2 Mintues",
    '300': "Last 5 Mintues",
    '600': "Last 10 Mintues",
    '1800': "Last 30 Mintues",
    '3600': "Last 1 Hour",
    '1440': "Last 4 Hours",
    '86400': "Last 1 Day",
}

@app.route("/")
@app.route("/index.html")
def _root():
    try:
        return render_template('index.html')
    except:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/top.html")
def _top():
    try:
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
        return render_template('top.html', locations=locations, interval=interval, location=location, servers=servers, client_ips=client_ips, status_codes=status_codes, intervals=INTERVALS)
    except:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/middle.html")
def _middle():
    try:
        return render_template('middle.html', locations=get_locations().keys())
    except:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/bottom.html")
def _bottom():
    try:
        return render_template('bottom.html', locations=get_locations())
    except:
        return Response(format_exc(), 500, content_type="text/plain")


@app.route("/get_data")
def _get_data():

    try:
        data = get_data(request.args)

        # Don't let the browser cache response
        response_headers = {
           'Access-Control-Allow-Origin': '*',
           'Cache-Control': 'no-cache, no-store',
           'Pragma': 'no-cache'
        }
        return jsonify(data), response_headers
    except:
        return Response(format_exc(), 500, content_type="text/plain")


if __name__ == '__main__':
    app.run()
