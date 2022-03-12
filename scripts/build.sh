#!/bin/bash
APP=$1
go mod vendor && go build -a -o ./dist/${APP} ./cmd/${APP}
