FROM rocker/shiny

RUN apt-get update
RUN apt-get install -y curl wget git libpq-dev ssh

## User variables
ARG SHINY_USER

ARG GITHUB_REPO
ARG GITHUB_USER

RUN mkdir /home/${SHINY_USER}/.ssh

## Adding keys for github
ADD id_rsa /home/${SHINY_USER}/.ssh/id_rsa

RUN touch /home/${SHINY_USER}/.ssh/known_hosts

RUN ssh-keyscan -T 60 github.com >> /home/${SHINY_USER}/.ssh/known_hosts

## Fix file's permissions
RUN chown -R ${SHINY_USER}:users -R /home/${SHINY_USER}

WORKDIR /home/${SHINY_USER}

USER shiny

## Clone the repository
RUN echo git clone git@github.com:$GITHUB_USER/$GITHUB_REPO.git

RUN git clone git@github.com:$GITHUB_USER/$GITHUB_REPO.git

WORKDIR /home/$SHINY_USER/$GITHUB_REPO

## Execute packrat
##RUN R -e "packrat::restore()"

USER root

EXPOSE 80

CMD ["/usr/bin/shiny-server.sh"]
