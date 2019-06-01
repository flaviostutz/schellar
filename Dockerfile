FROM golang:1.10 AS BUILD

#doing dependency build separated from source build optimizes time for developer, but is not required
#install external dependencies first
ADD /main.dep $GOPATH/src/schellar/main.go
RUN go get -v schellar

#now build source code
ADD schellar $GOPATH/src/schellar
RUN go get -v schellar



FROM golang:1.10 AS IMAGE

COPY --from=BUILD /go/bin/* /bin/
ADD startup.sh /

ENV CONDUCTOR_API_URL ''
ENV MONGO_ADDRESS ''
ENV MONGO_USERNAME ''
ENV MONGO_PASSWORD ''

CMD [ "/startup.sh" ]

# FROM BUILD AS TEST
# RUN go test -v schelly
