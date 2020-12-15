FROM gcr.io/deeplearning-platform-release/base-cpu
LABEL maintainer="amiyaguchi@mozilla.com"

# The home directory on a fresh ai-platform notebook is /home/jupyter, but it
# will be overwritten by the installation scripts. We'll make a new directory.
WORKDIR /app
COPY . /app
