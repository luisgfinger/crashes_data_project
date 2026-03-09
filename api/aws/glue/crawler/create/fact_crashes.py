from api.aws.glue.crawler.functions import GlueManager

glue = GlueManager()

glue.create_crawler(
    name="fact_crashes",
    s3_path="s3://crashes-data-luis-007/data_lake/gold/fact/fact_crash/",
)
