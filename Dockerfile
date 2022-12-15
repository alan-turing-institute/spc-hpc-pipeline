FROM ubuntu:20.04 
SHELL ["/bin/bash", "-c"]
VOLUME /home/docker
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get install -y build-essential manpages-dev zip unzip git wget sudo 
RUN adduser --disabled-password --gecos '' docker
RUN adduser docker sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER docker
RUN mkdir -p /home/docker 
RUN chmod 777 /home/docker
RUN cd /home/docker
ADD scripts/scp/* /home/docker
ADD data/* /home/docker
RUN sed -i 's/x86_64/aarch64/' /home/docker/SPENSER_HPC_setup.sh  
WORKDIR /home/docker/
ENTRYPOINT [ "bash", "/home/docker/SPENSER_HPC_setup.sh", "W06000019"]
# To run: 
# docker build -t "dyme-spc:Dockerfile" .
# docker run -d -t "dyme-spc:Dockerfile"
