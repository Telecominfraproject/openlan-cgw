FROM ubuntu

COPY ./requirements.txt /tmp/requirements.txt
WORKDIR /opt/client_simulator

RUN apt-get update -q -y  && apt-get -q -y --no-install-recommends install \
	python3 \
	python3-pip
RUN python3 -m pip install -r /tmp/requirements.txt

