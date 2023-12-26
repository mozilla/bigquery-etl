FROM python:3.8
MAINTAINER <simon@mozilla.com>

ARG HOME="/janus_build"
WORKDIR ${HOME}

RUN apt update && apt --yes install curl

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH $HOME/.cargo/bin:$PATH

RUN git clone --depth 1 https://github.com/divviup/janus.git --branch '0.6.6'
RUN cd janus && cargo build -r -p janus_tools --bin collect

######### next stage

FROM python:3.8
MAINTAINER <simon@mozilla.com>

ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"
WORKDIR ${HOME}

RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

COPY --from=0 /janus_build/janus/target/release/collect ./

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN pip install --no-cache-dir .
