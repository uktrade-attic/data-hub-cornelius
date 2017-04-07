FROM python:3.5

RUN mkdir -p /app
RUN mkdir -p /app/requirements
WORKDIR /app

ADD requirements/scraper.txt /app/requirements/
RUN pip install -r /app/requirements/scraper.txt

ADD . /app/

ADD docker/scraper/start.sh /app/
RUN chmod a+x /app/start.sh
CMD ./start.sh
