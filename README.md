# Airflow Docker Image
This is based off the airflow [quickstart guide](https://airflow.apache.org/docs/apache-airflow/2.0.1/start/docker.html). It is only meant for local dev purposes only.

> If you want a simple Docker-based deployment, consider using Kubernetes and deploying Airflow using the Official Airflow Community Helm Chart.
> https://airflow.apache.org/docs/helm-chart/stable/index.html


# Prerequsites
> Before you begin
> https://airflow.apache.org/docs/helm-chart/stable/index.html

# Init Environment
> ```
> mkdir -p ./dags ./logs ./plugins
> echo -e "AIRFLOW_UID=$(id -u)" > .env
> ```
> https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#setting-the-right-airflow-user

# Initialize the database
> ```
> docker-compose up airflow-init
> ```

# Clean up environment
> https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#cleaning-up-the-environment

# Running Airflow
> ```
> docker-compose up
> ```

Daemon
```
docker-compose up --build -d
```

Install dependencies
```
docker exec -it airflow-docker_airflow-worker_1 pip install -r requirements.txt
```
bash exec -it
```
docker exec -it airflow-docker_airflow-webserver_1 /bin/bash
```



# Stopping Airflow
## docker compose down
```
docker-compose down --volumes
```
to remove images, add `--rmi all` 
# stop all docker containers
```
docker kill $(docker ps -q)
```
# remove all docker containers
```
docker rm $(docker ps -a -q) 
```
# prune all volumes
```
docker system prune -a --volumes
```