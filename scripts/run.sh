#!/bin/bash
PACKAGE=github.com/cyradin/search
APP=${PACKAGE}/cmd/$1

go run ${APP} "${@:2}"