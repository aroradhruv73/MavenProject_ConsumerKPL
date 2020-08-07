FROM maven:3.6.3-openjdk-8 as builder
WORKDIR /usr/src

ENV MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"

# install maven dependency packages (keep in image)
COPY pom.xml /usr/src
RUN mvn dependency:go-offline

# copy other source files (keep in image)
COPY . /usr/src/

RUN mvn clean package

FROM openjdk:8-jdk-alpine

WORKDIR /usr/src

RUN apk update && \
apk upgrade && \
apk add bash gcc

RUN wget -O /usr/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 && \
chmod +x /usr/bin/dumb-init

ENV PORT 8080

# Expose port
EXPOSE ${PORT}
COPY --from=builder /usr/src/target/*.jar /usr/src/app.jar
COPY --from=builder /usr/src/dhruv_keystore.jks /usr/src/dhruv_keystore.jks

RUN ls

RUN chmod +x app.jar
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD ["java", "-Duser.timezone=America/New_York", "-jar", "java -cp /usr/src/app.jar EMAConsumer.Consumer"]