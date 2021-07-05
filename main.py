import json
import requests
import boto3

from datetime import datetime

BASE = "https://apitempo.inmet.gov.br/estacao/dados/"

def writeToStream(messages, stream_name):
    client = boto3.client('kinesis')
    for message in messages:
        client.put_record(
            StreamName=stream_name,
            Data=json.dumps(message),
            PartitionKey='part_key')


def getDateAndTime():
    now = datetime.now()
    now = now.strftime('%Y-%m-%d %H%M')
    date = now.split()[0]
    time = now.split()[1]
    if time[:2] != '0' and time[:3] != '0':
        time = list(time)
        time[2] = time[3] = '0'
        time = ''.join(time)
    return [date, time]


def extractFeatures(obj):
    dic = {}
    dic['CHUVA'] = obj['CHUVA']
    dic['UMD_MAX'] = obj['UMD_MAX']
    dic['TEM_MIN'] = obj['TEM_MIN']
    dic['UMD_INS'] = obj['UMD_INS']
    dic['RAD_GLO'] = obj['RAD_GLO']
    dic["CD_ESTACAO"] = obj["CD_ESTACAO"]
    dic["VL_LONGITUDE"] = obj["VL_LONGITUDE"]
    dic["VL_LATITUDE"] = obj["VL_LATITUDE"]
    dic["DC_NOME"] = obj["DC_NOME"]
    return dic


def getResponse(parameters):
    response = requests.get(BASE + parameters["data"] + "/" + parameters["hora"])
    return response


def filterResponse(response):
    cidades = response.json()
    result = []
    for cidade in cidades:
        if cidade['UF'] == 'PE':
            result.append(extractFeatures(cidade))
    return result


def init():
    lista_tempo = getDateAndTime()
    parameters = {
        'data': lista_tempo[0],
        'hora': lista_tempo[1]
    }
    response = getResponse(parameters)
    data = filterResponse(response)
    return data


def lambda_handler(event=None, context=None):
    name_stream = 'data_stream'
    messages = init()
    writeToStream(messages, name_stream)

    return {
        'statusCode': 200,
        'body': init()
    }
print(init())