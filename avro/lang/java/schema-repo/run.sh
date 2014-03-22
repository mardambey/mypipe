#!/bin/bash

#MAVEN_REPO=$HOME/.m2/repository

java -cp bundle/target/avro-repo-bundle-1.7.5-1124-SNAPSHOT-withdeps.jar org.apache.avro.repo.server.RepositoryServer config.properties
