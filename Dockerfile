FROM python:3.12-alpine
ENV PORT=8080
ENV APP_DIR=/opt
ENV PIP_ROOT_USER_ACTION=ignore
WORKDIR /tmp
COPY requirements.txt ./
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY static/ $APP_DIR/static/
COPY templates/ $APP_DIR/templates/
COPY *.py $APP_DIR/
COPY *.toml $APP_DIR/
COPY *.json $APP_DIR/
ENTRYPOINT cd $APP_DIR && gunicorn -b 0.0.0.0:$PORT -w 1 --access-logfile '-' wsgi:app
#ENTRYPOINT ["pip", "list"]
EXPOSE $PORT
