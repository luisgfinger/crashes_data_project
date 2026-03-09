from api.aws.glue.crawler.functions import GlueManager

glue = GlueManager(region="sa-east-1")

crawler_name = "fact_crashes"

glue.run_crawler(crawler_name)