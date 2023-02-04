from prefect.deployments import Deployment
from flows.etl_to_gcs import etl_to_gcs
from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_to_gcs,
    name="docker-hw-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()
