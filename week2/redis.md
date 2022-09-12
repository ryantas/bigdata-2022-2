1.- getting started redis

* Install redis [tutorial](https://redis.io/docs/getting-started/installation/install-redis-on-linux/) and verify the status using`systemctl status redis`.

2.-  Use Nosql databases as containers

2.1.- Install Docker and Docker Compose.

2.3.- Launch containers with docker-compose.yml file:

* [docker installation](https://docs.docker.com/engine/install/ubuntu/)
* [docker compose installation](https://docs.docker.com/compose/install/linux/)

Disable or stop existing deamon of redis or mongo with `sudo service redis stop`

```bash
docker-compose up -d
```

or run only 1 service, redis for instance,

```bash
docker-compose run -d --rm  --name myredis -p 6379:6379 redis
```

2.4.- Get into redis for testing.

```bash
docker exec -it myredis bash
redis-client
```

2.5 Test a python file with the running nosql database containers.

3.- referencias

* nosql docker  [steps](https://gist.github.com/andfanilo/fa2ec0577868014878a5079c276221ac)
