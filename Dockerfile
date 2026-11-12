FROM alpine:3.20 AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY Makefile ./
COPY src ./src
COPY bench ./bench
RUN make release

FROM alpine:3.20
COPY --from=build /src/build/release/pubsub-server /usr/local/bin/pubsub-server
EXPOSE 7400
USER nobody
ENTRYPOINT ["pubsub-server"]
CMD ["-p", "7400"]
