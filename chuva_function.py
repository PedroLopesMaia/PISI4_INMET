import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    s3_client = boto3.client('s3')
    s3_clientobj = s3_client.get_object(Bucket='weather-pernambuco2', Key='data.json')
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
    s3clientlist=json.loads(s3_clientdata)
    response = s3clientlist["CHUVA"]
    return { 'VALOR_OBSERVADO': response }
