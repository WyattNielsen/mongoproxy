#!/bin/sh -e

wget -q -O artifact_go.sh 'https://raw.githubusercontent.com/WyattNielsen/tools/master/artifact/artifact.sh'
chmod +x artifact_go.sh

. ./version.sh
./artifact_go.sh go
