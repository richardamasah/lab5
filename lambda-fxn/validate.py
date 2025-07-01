import json
import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Step Function client
sfn = boto3.client('stepfunctions')


STEP_FUNCTION_ARN = 'arn:aws:states:eu-north-1:992382846559:stateMachine:lakehouse-pipeline-stepfxn'

def lambda_handler(event, context):
    try:
        logger.info("Event received: %s", json.dumps(event))

        if 'Records' not in event:
            logger.error("No S3 Records in event")
            return {"status": "No records"}

        for record in event['Records']:
            s3_info = record['s3']
            bucket = s3_info['bucket']['name']
            key = s3_info['object']['key']
            logger.info(f"File uploaded: s3://{bucket}/{key}")

            # Check if it's a CSV
            if not key.endswith('.csv'):
                logger.warning(f"Invalid format, not CSV: {key}")
                return

            # Check if filename contains expected year 2025
            if '2025' not in key:
                logger.warning(f"Filename missing year 2025: {key}")
                return

            # Trigger Step Function
            logger.info("File is valid. Starting Step Function...")
            sfn.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps({"source_file": key})
            )
            logger.info("Step Function execution started")

        return {"status": "done"}

    except Exception as e:
        logger.exception("Error in Lambda validation function")
        return {
            "status": "error",
            "message": str(e)
        }
