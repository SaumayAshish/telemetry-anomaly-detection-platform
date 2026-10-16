# syntax=docker/dockerfile:1
ARG JAVA_VERSION=26

FROM eclipse-temurin:${JAVA_VERSION}-jdk AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN sed -i 's/\r$//' mvnw && chmod +x mvnw
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -q -DskipTests -pl ${MODULE} -am package \
    && mkdir /out \
    && cp ${MODULE}/target/*-SNAPSHOT.jar /out/app.jar \
    && if [ -d ${MODULE}/target/lib ]; then cp -r ${MODULE}/target/lib /out/lib; fi

FROM eclipse-temurin:${JAVA_VERSION}-jre
ENV TZ=UTC
RUN groupadd --system app && useradd --system --gid app --uid 10001 --no-create-home app \
    && mkdir /app /data && chown app:app /app /data
WORKDIR /app
COPY --from=build --chown=app:app /out/ /app/
USER app
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
