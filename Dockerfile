FROM python:3.7-slim

RUN useradd -ms /bin/bash ivend
USER ivend
ENV HOME /home/ivend
ENV PATH $PATH:$HOME/.local/bin

RUN mkdir /home/ivend/agent && \
  mkdir /home/ivend/repo && \
  chown -R ivend:ivend /home/ivend

COPY --chown=ivend:ivend requirements.txt /home/ivend/repo/
RUN pip install --user --requirement /home/ivend/repo/requirements.txt

COPY --chown=ivend:ivend . /home/ivend/repo/
RUN pip install --user --editable /home/ivend/repo

WORKDIR /home/ivend/agent
