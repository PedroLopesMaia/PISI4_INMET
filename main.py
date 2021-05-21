import requests

from datetime import datetime

BASE = "https://apitempo.inmet.gov.br/estacao/dados/"

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
    return dic


def getResponse(parameters):
    response = requests.get(BASE+parameters["data"]+"/"+parameters["hora"])
    return response

def filterResponse(response):
    cidades = response.json()
    #cidades = json.loads(jstring)
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

print(init())