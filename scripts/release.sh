#!/bin/bash

DATE=$(date +%Y-%m-%d-%H%M)

mkdir -p /tmp/$DATE

cd /tmp/$DATE
git clone git@github.com:IMGGaming/dce-tracking.git

cd dce-tracking
echo release build in $(pwd)
mvn -B org.apache.maven.plugins:maven-release-plugin:2.5.3:clean org.apache.maven.plugins:maven-release-plugin:2.5.3:prepare org.apache.maven.plugins:maven-release-plugin:2.5.3:perform
#mvn release:clean release:prepare release:perform
