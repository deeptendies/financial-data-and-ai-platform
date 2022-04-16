# Financial Data and AI Platform
## Intro
A data platform for ingesting, transforming data, and run financial and analytical jobs.

maintained by
[@micdean19](https://github.com/micdean19) 
[@MichaelKissinger](https://github.com/MichaelKissinger) 
[@mklasby](https://github.com/mklasby) 
[@markomijovic](https://github.com/markomijovic) 
[@stancsz](https://github.com/stancsz)
[@erikawyt](https://github.com/erikawyt)

## Technology
A data platform consist of 
Modules
- Airflow
- Spark
- Kafka

Jobs
- airflow dags
- spark jobs
- Kafka streaming apps

Training: 

https://github.com/stancsz/training-data-platform-engineer

# Setup Instruction
1. fulfill docker + docker-compose prerequisites in the airflow
2. run the following code to export environment variables for airflow 
```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
# AIRFLOW_GID=0 may be needed
echo -e "AIRFLOW_GID=0" >> .env
```
3. init airflow
```
docker-compose up --build airflow-init
```
4. execute docker-compose to build the services in this stack
```
docker-compose up
```
if running in daemon is preferred
```
docker-compose up -d
```

## Airflow UI
http://localhost:8080/



# Docker Example
```
$ docker-compose up
WARNING: Found orphan containers (base-data-platform-stack_spark-master_1) for this project. If you removed or renamed this service in your compose file, you can run this command with the --remove-orphans flag to clean it up.
Creating base-data-platform-stack_spark-worker-node-1_1 ... done
Creating base-data-platform-stack_spark-worker-node-2_1 ... done
Creating base-data-platform-stack_all-spark-notebook_1  ... done
Creating base-data-platform-stack_spark_1               ... done
Creating base-data-platform-stack_redis_1               ... done
Creating base-data-platform-stack_postgres_1            ... done
Creating base-data-platform-stack_airflow-init_1        ... done
Creating base-data-platform-stack_flower_1              ... done
Creating base-data-platform-stack_airflow-scheduler_1   ... done
Creating base-data-platform-stack_airflow-triggerer_1   ... done
Creating base-data-platform-stack_airflow-webserver_1   ... done
Creating base-data-platform-stack_airflow-worker_1      ... done
Attaching to base-data-platform-stack_postgres_1, base-data-platform-stack_spark-worker-node-2_1, base-data-platform-stack_spark-worker-node-1_1, base-data-platform-stack_redis_1, base-data-platform-stack_spark_1, base-data-platform-stack_all-spark-notebook_1, base-data-platform-stack_airflow-init_1, base-data-pla
tform-stack_airflow-scheduler_1, base-data-platform-stack_airflow-triggerer_1, base-data-platform-stack_airflow-worker_1, base-data-platform-stack_flower_1, base-data-platform-stack_airflow-webserver_1
airflow-init_1         | The container is run as root user. For security, consider using a regular user account.

```

```
$ docker ps
CONTAINER ID   IMAGE                        COMMAND                  CREATED              STATUS                        PORTS                              NAMES
c792711cab79   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   About a minute ago   Up 41 seconds (healthy)       8080/tcp                           base-data-platform-stack_airflow-scheduler_1
b5d238244883   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   About a minute ago   Up 40 seconds (healthy)       0.0.0.0:5555->5555/tcp, 8080/tcp   base-data-platform-stack_flower_1
53a42b944ade   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   About a minute ago   Up 40 seconds (healthy)       8080/tcp                           base-data-platform-stack_airflow-worker_1
9f6efbc46a4c   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   About a minute ago   Up 40 seconds (healthy)       0.0.0.0:80->8080/tcp               base-data-platform-stack_airflow-webserver_1
9f5535841078   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   About a minute ago   Up 40 seconds (healthy)       8080/tcp                           base-data-platform-stack_airflow-triggerer_1
e53b7e0d509c   bitnami/spark:3              "/opt/bitnami/script…"   2 minutes ago        Up About a minute             0.0.0.0:8080->8080/tcp             base-data-platform-stack_spark-master_1
aa3cde52b858   jupyter/all-spark-notebook   "tini -g -- jupyter …"   2 minutes ago        Up About a minute             0.0.0.0:8888->8888/tcp             base-data-platform-stack_all-spark-notebook_1
9aea822f478a   bitnami/spark:3              "/opt/bitnami/script…"   2 minutes ago        Up About a minute                                                base-data-platform-stack_spark-worker-node-2_1
eac32a6d4bc3   postgres:13                  "docker-entrypoint.s…"   2 minutes ago        Up About a minute (healthy)   5432/tcp                           base-data-platform-stack_postgres_1
7a1ff9a55076   redis:latest                 "docker-entrypoint.s…"   2 minutes ago        Up About a minute (healthy)   6379/tcp                           base-data-platform-stack_redis_1
5a88cd517f7b   bitnami/spark:3              "/opt/bitnami/script…"   2 minutes ago        Up About a minute                                                base-data-platform-stack_spark-worker-node-1_1
```


# Stopping Services & Clean up environment
> https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#cleaning-up-the-environment

Stopping Airflow with docker compose down
```
docker-compose down
```
to remove images, `docker-compose down --volumes` and add `--rmi all` 

_additional commands:_

stop all docker containers
```
docker kill $(docker ps -q)
```
remove all docker containers
```
docker rm $(docker ps -a -q) 
```
prune all volumes
```
docker system prune -a --volumes
```
