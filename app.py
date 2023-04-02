from quart import Quart, request, render_template, Response, jsonify
from main import *
from traceback import format_exc

app = Quart(__name__, static_url_path='/static')
app.config['JSON_SORT_KEYS'] = False


@app.route("/")
@app.route("/index.html")
def _root():
    try:
        return render_template('squid.html')
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


@app.route("/squid_top.html")
def _top():
    try:
        return render_template('squid_top.html', locations=get_locations(), servers=get_servers())
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


@app.route("/squid_middle.html")
def _middle():
    try:
        locations = get_locations()
        return render_template('squid_middle.html', locations=locations.keys())
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


@app.route("/squid_bottom.html")
def _bottom():
    try:
        return render_template('squid_bottom.html', locations=get_locations())
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
    except Exception as e:
        return Response(format(e), 500, content_type="text/plain")


if __name__ == '__main__':
    app.run()

