FROM --platform=linux/amd64 ubuntu:20.04 
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
WORKDIR /home/docker/
ENTRYPOINT [ "bash", "/home/docker/SPENSER_HPC_setup.sh" ]
# To run: 
# docker build -t "dyme-spc:Dockerfile" .
# docker run -d -t "dyme-spc:Dockerfile"