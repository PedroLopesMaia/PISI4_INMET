import base64
import json
import requests
import boto3

from datetime import datetime

BASE = "https://apitempo.inmet.gov.br/estacao/dados/"

s3_client = boto3.client('s3')

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

def getRef():
    try:
        s3_clientobj = s3_client.get_object(Bucket='weather-pernambuco2', Key='data.json')
        s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
        s3clientlist=json.loads(s3_clientdata)
        return s3clientlist
    except:
        return {}

def create(in_data):
    result = {'CHUVA': in_data['CHUVA'], 'UMD_INS': in_data['UMD_INS'], 'RAD_GLO': in_data['RAD_GLO']}
    raw_data = init()
    for obj in raw_data:
        if obj['TEM_MIN'] != None:
            if float(obj['TEM_MIN']) == in_data['TEM_MIN']:
                result['TEM_MIN'] = {}
                result['TEM_MIN']['VALOR_OBSERVADO'] = obj['TEM_MIN']
                result['TEM_MIN']['CODIGO_ESTACAO'] = obj['CD_ESTACAO']
                result['TEM_MIN']['NOME_ESTACAO'] = obj['DC_NOME']
                result['TEM_MIN']['LATITUDE'] = obj['VL_LATITUDE']
                result['TEM_MIN']['LONGITUDE'] = obj['VL_LONGITUDE']
                result['TEM_MIN']['HORARIO_COLETA'] = str(datetime.now()).split()[1][:8]
        if obj['UMD_MAX'] != None:
            if int(obj['UMD_MAX']) == in_data['UMD_MAX']:
                result['UMD_MAX'] = {}
                result['UMD_MAX']['VALOR_OBSERVADO'] = obj['UMD_MAX']
                result['UMD_MAX']['CODIGO_ESTACAO'] = obj['CD_ESTACAO']
                result['UMD_MAX']['NOME_ESTACAO'] = obj['DC_NOME']
                result['UMD_MAX']['LATITUDE'] = obj['VL_LATITUDE']
                result['UMD_MAX']['LONGITUDE'] = obj['VL_LONGITUDE']
                result['UMD_MAX']['HORARIO_COLETA'] = str(datetime.now()).split()[1][:8]
    return result

def update(ref_data, in_data):
    if bool(ref_data):
        result = ref_data
        raw_data = init()
        result['CHUVA'] = float(in_data['CHUVA']) + float(result['CHUVA'])
        result['UMD_INS'] = (float(in_data['UMD_INS']) + float(result['UMD_INS']))/2
        result['RAD_GLO'] = float(in_data['RAD_GLO']) + float(result['RAD_GLO'])

        if float(ref_data['TEM_MIN']['VALOR_OBSERVADO']) > float(in_data['TEM_MIN']):
            for obj in raw_data:
                if obj['TEM_MIN'] != None:
                    if float(obj['TEM_MIN']) == float(in_data['TEM_MIN']):
                        result['TEM_MIN']['VALOR_OBSERVADO'] = obj['TEM_MIN']
                        result['TEM_MIN']['CODIGO_ESTACAO'] = obj['CD_ESTACAO']
                        result['TEM_MIN']['NOME_ESTACAO'] = obj['DC_NOME']
                        result['TEM_MIN']['LATITUDE'] = obj['VL_LATITUDE']
                        result['TEM_MIN']['LONGITUDE'] = obj['VL_LONGITUDE']
                        result['TEM_MIN']['HORARIO_COLETA'] = str(datetime.now()).split()[1][:8]
                        break
        if int(ref_data['UMD_MAX']['VALOR_OBSERVADO']) < int(in_data['UMD_MAX']):
            for obj in raw_data:
                if obj['UMD_MAX'] != None:
                    if int(obj['UMD_MAX']) == int(in_data['UMD_MAX']):
                        result['UMD_MAX']['VALOR_OBSERVADO'] = obj['UMD_MAX']
                        result['UMD_MAX']['CODIGO_ESTACAO'] = obj['CD_ESTACAO']
                        result['UMD_MAX']['NOME_ESTACAO'] = obj['DC_NOME']
                        result['UMD_MAX']['LATITUDE'] = obj['VL_LATITUDE']
                        result['UMD_MAX']['LONGITUDE'] = obj['VL_LONGITUDE']
                        result['UMD_MAX']['HORARIO_COLETA'] = str(datetime.now()).split()[1][:8]
                        break
        return result
    else:
        return create(in_data)

def upload(json_data):
    s3_client.put_object(
        Body=str(json.dumps(json_data)),
        Bucket='weather-pernambuco2',
        Key='data.json'
    )

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        payload = json.loads(payload)
        reference = getRef()
        result = update(reference, payload)
        upload(result)
    return result