# Airflow Docker Image with Starter Dag
## What is this repo?
This is a starter repo to get started with airflow, and to create your first DAGs with examples on how to dynamically generate them.

![](Airflow_README/img.png)
![img_1.png](Airflow_README/img2.png)

This is based off the airflow [quickstart guide](https://airflow.apache.org/docs/apache-airflow/2.0.1/start/docker.html). It is only meant for local dev purposes only. This image extends the official image to allow installation of images during setup, with dependencies added in the `requirements.txt`. This setup is also meant for users who would like to have a quick and convenient local environment for developing dags meant to deployed to Google Cloud Composer or Astronomer. 

> If you want a simple Docker-based deployment, consider using Kubernetes and deploying Airflow using the Official Airflow Community Helm Chart.
> https://airflow.apache.org/docs/helm-chart/stable/index.html

## Prerequsites
> Before you begin
> https://airflow.apache.org/docs/helm-chart/stable/index.html

1. get docker. 
```
sudo curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
```
For Windows or Mac OS, please get Docker Desktop from https://www.docker.com/products/docker-desktop

2. get docker compoose
```
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
sudo docker-compose --version
```
For Windows or Mac OS, install Compose https://docs.docker.com/compose/install/

# Setup
> ```
> mkdir -p ./dags ./logs ./plugins
> echo -e "AIRFLOW_UID=$(id -u)" > .env
> # AIRFLOW_GID=0 may be needed
> echo -e "AIRFLOW_GID=0" >> .env
> ```
> https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#setting-the-right-airflow-user


# Start containers
Initialize the database
> ```
> docker-compose up --build airflow-init
> ```

Running Airflow
> ```
> docker-compose up
> ```

# Working with pods
Install additional dependencies
```
docker exec -it airflow-docker_airflow-worker_1 pip install -r requirements.txt
```
bash exec -it
```
docker exec -it airflow-docker_airflow-webserver_1 /bin/bash
```


# Clean up environment
> https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#cleaning-up-the-environment

Stopping Airflow with docker compose down
```
docker-compose down --volumes
```
to remove images, add `--rmi all` 

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
