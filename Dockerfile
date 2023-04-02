FROM python:3.11-slim-bullseye
MAINTAINER johnnylingo
ENV PORT=8080
ENV APP_DIR=/opt
ENV WSGI_APP=wsgi:app
WORKDIR /tmp
COPY requirements.txt ./
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY *.py $APP_DIR/
COPY locations.toml APP_DIR/
COPY *.json $APP_DIR/
ENTRYPOINT gunicorn -b 0.0.0.0:$PORT -w 1 --chdir=$APP_DIR --access-logfile '-' $WSGI_APP
EXPOSE $PORT