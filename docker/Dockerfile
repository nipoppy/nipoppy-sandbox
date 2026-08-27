FROM neurodebian:bookworm-non-free

# set up dependency versions
ARG GO_VERSION=1.23.6
ARG APPTAINER_VERSION=1.4.1
ARG OS=linux
ARG ARCH=amd64

# install dependencies
RUN	apt update && apt upgrade && apt install -y autoconf automake build-essential cryptsetup cryptsetup-bin curl dh-apparmor fakeroot fuse2fs git fuse libfuse-dev libgpgme-dev libseccomp-dev libsubid-dev libtool pkg-config python3 python3.11-venv runc squashfs-tools squashfs-tools-ng tzdata uidmap uuid-dev wget zlib1g-dev

#
# install go from precompiled binary
#

RUN	wget https://dl.google.com/go/go$GO_VERSION.$OS-$ARCH.tar.gz
RUN	tar -C /usr/local -xzvf go$GO_VERSION.$OS-$ARCH.tar.gz
RUN	rm go$GO_VERSION.$OS-$ARCH.tar.gz

ENV GOPATH=/usr/local/go
ENV PATH=/usr/local/go/bin:${PATH}

#
# install apptainer from source
#

ARG MCONFIG_OPTIONS="--without-suid"
WORKDIR $GOPATH/src/github.com/apptainer

RUN git clone https://github.com/apptainer/apptainer.git $GOPATH/src/github.com/apptainer
RUN cd $GOPATH/src/github.com/apptainer
RUN git checkout "v$APPTAINER_VERSION"

RUN	./mconfig $MCONFIG_OPTIONS -p /usr/local/apptainer
RUN	make -C $GOPATH/src/github.com/apptainer/builddir
RUN make -C $GOPATH/src/github.com/apptainer/builddir install

ENV PATH=/usr/local/apptainer/bin:$PATH
RUN rm -rf $GOPATH/src/github.com/apptainer

#
# install nipoppy
#

# create and activate venv
RUN	python3 -m venv /nipoppy
ENV PATH=/nipoppy/bin:$PATH

# install nipoppy
RUN	pip install --upgrade pip setuptools wheel
RUN	pip install nipoppy[all]==0.4.0

#
# set up container
#

# set default directory to root
WORKDIR /

# set entry command to nipoppy
CMD	["nipoppy"]
