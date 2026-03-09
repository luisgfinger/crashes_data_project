import boto3
from botocore.exceptions import ClientError


class GlueManager:

    def __init__(self, region="sa-east-1"):
        self.client = boto3.client("glue", region_name=region)

    def create_crawler(self, name, s3_path, role_arn = "arn:aws:iam::674594306997:role/service-role/AWSGlueServiceRole-crawler_IAM", db_name="crashes_nyc_db", schedule=None):

        params = {
            "Name": name,
            "Role": role_arn,
            "DatabaseName": db_name,
            "Targets": {
                "S3Targets": [
                    {"Path": s3_path}
                ]
            },
            "SchemaChangePolicy": {
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG"
            }
        }

        if schedule:
            params["Schedule"] = schedule

        try:
            self.client.create_crawler(**params)
            print(f"Crawler '{name}' successfully created.")
        except ClientError as e:
            print("Error:", e.response["Error"]["Message"])
    
    def run_crawler(self, name):
        try:
            self.client.start_crawler(Name=name)
            print(f"Crawler '{name}' started.")
        except ClientError as e:
            print("Error:", e.response["Error"]["Message"])