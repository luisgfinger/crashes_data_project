from api.aws.glue.crawler.functions import GlueManager

glue = GlueManager(region="sa-east-1")

crawler_name = "dim_vehicles"

glue.run_crawler(crawler_name)