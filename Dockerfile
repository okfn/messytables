FROM ubuntu:14.04

RUN apt-get update && \
    apt-get install -y \
        python-pip \
        python-dev

RUN apt-get install -y python-numpy python-lxml python-chardet
RUN apt-get install -y python3 python3-pip python3-lxml python3-nose
RUN apt-get install -y python-nose
RUN locale-gen en_GB.UTF-8

RUN mkdir /home/messytables && \
    chown nobody /home/messytables
USER nobody
ENV HOME=/home/messytables \
    PATH=/home/messytables/.local/bin:$PATH \
    LANG=en_GB.UTF-8
# LANG needed for httpretty install on Py3
WORKDIR /home/messytables

COPY ./requirements-test.txt /home/messytables/

RUN pip install --user -r /home/messytables/requirements-test.txt
RUN pip3 install --user -r /home/messytables/requirements-test.txt
COPY . /home/messytables/
