<img align="left" width="300" src="https://github.com/PedroLopesMaia/PISI4_INMET/assets/34479719/1235a399-af2f-4d22-b1c5-f524ce386e4c" />

# UFRPE-BSI 2020.1 | Projeto Interdiciplinar para Sistemas de Informação IV

O projeto consistiu no desenvolvimento de um sistema distribuído em nuvem, utilizando alguns dos serviços disponíveis no AWS Educate,           
que são capazes de integrar dados meteorológicos de estações do Instituto Nacional de Meteorologia (INMET) existentes no estado de Pernambuco.

Cada uma das estações fornece dados de variáveis meteorológicas. As variáveis meteorológicas horárias são capturadas através de um serviço
web do próprio INMET implementado em uma API REST. Uma tarefa programada utilizando AWS Lambda e EasyCron.com foi criada para realizar a
chamada ao serviço web do INMET, de hora em hora, filtrar os dados e repassá-los para o serviço AWS Kinesis Data Streams. Em seguida, estes 
dados são processados pelo Kinesis Data Analytics. Os dados processados são então enviados ao Kinesis Data Firehose para persistência em um 
bucket S3. Os dados persistidos servirão como referência para o processamento no Kinesis Data Analytics e para a API REST de consulta aos 
dados que foi desenvolvida em outra função AWS Lambda.



