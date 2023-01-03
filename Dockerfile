FROM --platform=linux/arm64 ubuntu:20.04 
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
WORKDIR /home/docker/
# Comment out nextline if running on a non-arm system
RUN sed -i 's/x86_64/aarch64/' /home/docker/SPENSER_HPC_setup.sh  
RUN chmod +x /home/docker/SPENSER_HPC_setup.sh
ENTRYPOINT ["tail", "-f", "/dev/null"]

# To run all Wales LADs for example: 

# docker build -t "dyme-spc:Dockerfile" .
# docker run --name dyme -d -t "dyme-spc:Dockerfile"
# docker exec -it dyme bash ./SPENSER_HPC_setup.sh `awk -F "\"*,\"*" '{print substr($1,2)}' new_lad_list_Wales.csv | awk 'NR!=1 {print}'`