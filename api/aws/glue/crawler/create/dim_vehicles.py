from api.aws.glue.crawler.functions import GlueManager

glue = GlueManager()

glue.create_crawler(
    name="dim_vehicles",
    s3_path="s3://crashes-data-luis-007/data_lake/gold/dim/dim_vehicle/",
)
