FROM maven:3.8.5-openjdk-17 AS build

WORKDIR /app

COPY pom.xml .
COPY . .

RUN mvn clean package

ENTRYPOINT ["java", "-jar", "./target/influxdb-client-1.0-SNAPSHOT.jar"]