FROM ubuntu:18.04

######INSTALL PYTHON3.7##########
RUN apt update -y \
    && apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev wget tar curl make -y \
    && wget https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz \
    && tar -xf Python-3.7.4.tgz \
    && cd Python-3.7.4 \
    && ./configure --enable-optimizations \
    && make -j 8 \
    && make altinstall \
    && cp -R /usr/local/bin/python3.7 /usr/bin \
    && mv /usr/bin/python3.7 /usr/bin/python \
    && echo "PATH=$PATH:/usr/bin/python" > ~/.bashrc \
    && . ~/.bashrc \
    && python --version

###########INSTALL CLOUD CUSTODIAN FOR AZURE##########
RUN python -m venv custodian \
    && /bin/bash -c "source /custodian/bin/activate" \
    && python -m pip install c7n_azure  \
    && python -m pip install azure-cli