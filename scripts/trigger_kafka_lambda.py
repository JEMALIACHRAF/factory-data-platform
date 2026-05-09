"""
Invoque une Lambda dans le VPC pour envoyer des events vers MSK
"""
import boto3
import json

lambda_client = boto3.client('lambda', region_name='eu-west-1')

payload = {
    "topic": "factory-iot-events",
    "n_events": 100
}

response = lambda_client.invoke(
    FunctionName='factory-kafka-producer-prod',
    InvocationType='RequestResponse',
    Payload=json.dumps(payload)
)

result = json.loads(response['Payload'].read())
print(result)