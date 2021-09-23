# FMTOK8s Queue Service
For more information check [http://github.com/salaboy/from-monolith-to-k8s](http://github.com/salaboy/from-monolith-to-k8s)

This service implements a virtual Queueing System for people to queue to buy tickets for an event. 

## Example Flow

Join the queue to buy tickets:

```
curl -X POST http://localhost:8084/join -H "Content-Type: application/json" -H "ce-type: Queue.CustomerJoined"  -H "ce-id: 123"  -H "ce-specversion: 1.0" -H "ce-source: curl-command" -d '{"sessionId" : "123" }' 

```

You can also abandon the queue: 

```
curl -X POST http://localhost:8084/abandon -H "Content-Type: application/json" -H "ce-type: Queue.CustomerAbandoned"  -H "ce-id: 123"  -H "ce-specversion: 1.0" -H "ce-source: curl-command" -d '{"sessionId" : "123" }' 

```



## Build and Release

```
mvn package
```

```
docker build -t salaboy/fmtok8s-queue-service:0.1.0
docker push salaboy/fmtok8s-queue-servicet:0.1.0
```

```
cd charts/fmtok8s-queue-service
helm package .
```

Copy tar to http://github.com/salaboy/helm and push
