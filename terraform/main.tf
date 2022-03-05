terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 2.13.0"
    }
  }
}

provider "docker" {}

resource "docker_image" "spark" {
  name         = "bitnami/spark"
  keep_locally = "false"
}

resource "docker_container" "spark" {
  image = docker_image.spark.latest
  name  = "spark"
  env = [
    "SPARK_MODE=master",
    "SPARK_RPC_AUTHENTICATION_ENABLED=no",
    "SPARK_RPC_ENCRYPTION_ENABLED=no",
    "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no",
    "SPARK_SSL_ENABLED=no"
  ]
  restart = "always"
  ports {
    internal = 8080
    external = 8082
  }
}

resource "docker_image" "jupyter_asn" {
  name         = "jupyter/all-spark-notebook"
  keep_locally = "false"
}

resource "docker_container" "jupyter_asn" {
  image   = docker_image.jupyter_asn.latest
  name    = "jupyter_asn"
  restart = "always"
  env     = ["JUPYTER_ENABLE_LAB=yes"]
  command = ["jupyter", "lab", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]
  ports {
    internal = 8888
    external = 8888
  }
}