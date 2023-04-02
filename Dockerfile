FROM python:3.11-slim-bullseye
MAINTAINER johnnylingo
ENV PORT=8000
ENV WSGI_DIR=/opt
ENV WSGI_APP=wsgi:app
WORKDIR /tmp
COPY requirements.txt ./
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY *.py $WSGI_DIR/
COPY locations.toml $WSGI_DIR/
COPY *.json $WSGI_DIR/
ENTRYPOINT gunicorn -b 0.0.0.0:$PORT -w 1 --chdir=$WSGI_DIR --access-logfile '-' $WSGI_APP
EXPOSE $PORT

