FROM akamlani/package-java-oracle-1.8:master

WORKDIR /root
COPY    config/anaconda  config/anaconda

RUN apt-get update -y

# Default environment as Anaconda Python 3
ENV anaconda3_path=/root/anaconda3
ENV PATH=${anaconda3_path}/bin:$PATH
RUN \
    wget https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh \
    && bash Anaconda3-4.4.0-Linux-x86_64.sh -b -p ${anaconda3_path}  \
    && conda update  -y anaconda \
    && conda upgrade -y conda \
    && rm Anaconda3-4.4.0-Linux-x86_64.sh \
    && conda clean -yt

# Create Base Environments
RUN conda env create -f config/anaconda/techstarsiot.base.3.6_env.yaml

# Set default environment channel
ENV default_anaconda_env=techstarsiot.base.3.6
ENV PATH=${anaconda3_path}/envs/${default_anaconda_env}/bin:$PATH
RUN echo "source activate ${default_anaconda_env}" >> .bashrc
RUN /bin/bash -c "source ~/.bashrc"
RUN conda info -e

# Set default entrypoint
ENTRYPOINT ["/bin/bash"]
