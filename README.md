
## Before running

### 1.Add below  to your /etc/hosts
```
127.0.0.1	mongo1
127.0.0.1	mongo2
127.0.0.1	mongo3
```

#### 2. Run docker-compose
```sh
docker-compose up -d
```

## How to connect the mongodb with mongo client
```sh
mongo "mongodb://mongo1:30001,mongo2:30002,mongo3:30003/test_db?replicaSet=my-replica-set"
```

# Thanks
- https://www.upsync.dev/2021/02/02/run-mongo-replica-set.html
- https://github.com/UpSync-Dev/docker-compose-mongo-replica-set
