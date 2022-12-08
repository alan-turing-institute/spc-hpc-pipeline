FROM --platform=linux/amd64 ubuntu:20.04 
ADD scripts/scp/* ~/
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get install -y build-essential manpages-dev zip unzip git wget sudo 
RUN useradd -m docker && echo "docker:docker" | chpasswd && adduser docker sudo
USER docker
ENTRYPOINT [ "sh", "/SPENSER_HPC_setup.sh" ]