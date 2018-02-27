FROM         base/archlinux:latest 
MAINTAINER   Arnau Prat <arnau.prat@gmail.com>
CMD          bash

RUN mkdir -p /home/user
WORKDIR /home/user 

ADD src ./src 
ADD CMakeLists.txt ./
ADD libs ./libs 

RUN mkdir build 

# Required system packages
RUN pacman -Syy
RUN pacman -S --noconfirm gcc cmake numactl make boost boost-libs 
