"""
Script to start a Watson Studio pipeline job using the Watson Data API.

"""

import requests
from datetime import date
from dataclasses import dataclass


@dataclass
class JobRunner:
    api_key: str = os.environ['API_KEY']
    project_id: str = os.environ['PROJECT_ID']
    base_url: str = "https://api.dataplatform.cloud.ibm.com"
    identity_url: str = "https://iam.cloud.ibm.com/identity/token"
    job_name: str = "Trial job - sample_loan_risk_pipeline_stage"

    def create_access_token(self):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = f"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={self.api_key}"

        response = requests.post(self.identity_url, headers=headers, data=data)
        response.raise_for_status()

        return response.json()["access_token"]

    def list_jobs(self, access_token):
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"project_id": self.project_id, "limit": "100", "userfs": "false"}

        response = requests.get(f"{self.base_url}/v2/jobs", headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def retrieve_job_id(self, access_token: str) -> str:
        jobs = self.list_jobs(access_token)
        matching_jobs = [
            job for job in jobs["results"] if job["metadata"]["name"] == self.job_name
        ]
        if not matching_jobs:
            raise ValueError(f"No job found with name '{name}'")
        return matching_jobs[-1]["metadata"]["asset_id"]

    def run_pipeline_job(self, job_id: str, access_token: str):
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        params = {"project_id": self.project_id, "userfs": "false"}

        payload = {
            "job_run": {
                "name": f"API_Run_{date.today()}",
                "description": "Triggered via API"
            }
        }

        response = requests.post(
            f"{self.base_url}/v2/jobs/{job_id}/runs",
            headers=headers,
            params=params,
            json=payload
        )
        response.raise_for_status()
        return response.json()


def main():
    job_runner = JobRunner()
    access_token = job_runner.create_access_token()

    #jobs = job_runner.list_jobs(access_token=access_token)
    #print(jobs)
    
    job_id = job_runner.retrieve_job_id(access_token=access_token)
    print(f"Found Job ID: {job_id}")

    run_response = job_runner.run_pipeline_job(job_id=job_id, access_token=access_token)
    run_id = run_response["metadata"]["asset_id"]
    print(f"Started pipeline job successfully. Run ID: {run_id}")


if __name__ == "__main__":
    main()
