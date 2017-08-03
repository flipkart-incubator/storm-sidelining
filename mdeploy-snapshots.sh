#!/usr/bin/env bash
mvn clean source:jar deploy -DskipTests -DaltDeploymentRepository=flipkart::default::http://10.85.59.116/artifactory/v1.0/artifacts/libs-snapshot-local
mvn clean source:jar deploy -DskipTests -DaltDeploymentRepository=flipkart::default::http://10.85.59.116/artifactory/v1.0/artifacts/libs-snapshots-local
