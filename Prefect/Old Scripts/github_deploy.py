from prefect.deployments import Deployment
from etl_web_to_gcs_green import etl_web_to_gcs
from prefect.filesystems import GitHub

github_block = GitHub.load("green-gcs")

github_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="github-flow",
)

if __name__ == "__main__":
    github_dep.apply()
