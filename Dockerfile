FROM python:3.10
LABEL maintainer="Krzysztof Ignasiak <kik@mozilla.com>"

# https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
ENV PIP_VERSION=23.1.2

RUN groupadd --gid ${USER_ID} ${GROUP_ID} \
    && useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}

RUN python -m pip install --upgrade pip==${PIP_VERSION}

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN python -m pip install .

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}
