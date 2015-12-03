#!/bin/bash -eu

if grep -q SNAPSHOT "version.txt"; then
   echo "Publishing snapshot to internal artifactory"
   ./gradlew publish -PsmartThingsArtifactoryUserName=$ARTIFACTORY_USERNAME -PsmartThingsArtifactoryPassword=$ARTIFACTORY_PASSWORD
else
   echo "Publishing release version to bintray"
   ./gradlew bintrayUpload
fi
