import json
import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')

# s3 bucket settings
DATABASE = 'lakehouse_dwh'
OUTPUT_S3 = 's3://lab5lakehouse/query-results/'

def lambda_handler(event, context):
    try:
        logger.info("Received Athena trigger event: %s", json.dumps(event))

        query = event.get("query", "SELECT COUNT(*) FROM products")
        logger.info(f"Executing Athena query: {query}")

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': OUTPUT_S3
            }
        )

        execution_id = response['QueryExecutionId']
        logger.info(f"Athena query started. Execution ID: {execution_id}")

        return {
            "status": "started",
            "execution_id": execution_id
        }

    except Exception as e:
        logger.exception("Failed to execute Athena query")
        return {
            "status": "error",
            "message": str(e)
        }
