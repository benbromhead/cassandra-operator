FROM debian:stretch

COPY dagi /usr/local/bin/

RUN dagi locales gnupg2 dirmngr lsb-release curl \
    openjdk-8-jdk-headless maven

RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

RUN dagi google-cloud-sdk
