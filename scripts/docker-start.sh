#!/bin/sh

echo "Starting dev containers for USER=$(id -u) in ${PWD}"
cd ${PWD}/scripts

if [ -x "$(command -v docker-compose)" ]
then
    USER_ID=$(id -u) GROUP_ID=$(id -g) docker-compose up -d
else
    USER_ID=$(id -u) GROUP_ID=$(id -g) docker compose up -d
fi
