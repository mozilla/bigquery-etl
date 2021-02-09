FROM python:3.8
MAINTAINER Ben Wu <bewu@mozilla.com>

# https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

RUN pip install --upgrade pip

COPY requirements.txt ./
COPY requirements.dev.txt ./
RUN pip install -r requirements.dev.txt

WORKDIR ${HOME}

COPY requirements.txt ./
COPY requirements.dev.txt ./
RUN pip install -r requirements.dev.txt

COPY . .

RUN pip install .

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}
