#!/bin/bash
set -u
DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`
cd $DIR

/home/cc/maven/bin/mvn package -P spark3.0.3     


result=$?

if [ $result -ne 0 ]; then
    echo "Build failed, please check!"
else
    echo "Build all done!"
fi
