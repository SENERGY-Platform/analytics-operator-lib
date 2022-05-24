FROM maven:3.6-openjdk-11-slim as builder
ADD src /usr/src/app/src
ADD pom.xml /usr/src/app
WORKDIR /usr/src/app
RUN mvn clean install
RUN mvn install:install-file -Dfile=/usr/src/app/target/operator-lib-0.3-SNAPSHOT.jar -DpomFile=/usr/src/app/pom.xml
RUN rm -rf /usr/src/app
LABEL org.opencontainers.image.source https://github.com/SENERGY-Platform/analytics-operator-sum