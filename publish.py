import paho.mqtt.client as paho
from datetime import datetime, date
import os
import socket
import ssl
import random
import string
import json
from time import sleep
from random import uniform
from datetime import date, datetime

connflag = False


def on_connect(client, userdata, flags, rc):  # func for making connection
    global connflag
    print("Connected to AWS")
    connflag = True
    print("Connection returned result: " + str(rc))


def on_message(client, userdata, msg):  # Func for Sending msg
    print(msg.topic + " " + str(msg.payload))


# def on_log(client, userdata, level, buf):
#    print(msg.topic+" "+str(msg.payload))

mqttc = paho.Client()  # mqttc object
mqttc.on_connect = on_connect  # assign on_connect func
mqttc.on_message = on_message  # assign on_message func
# mqttc.on_log = on_log

#### Change following parameters ####
# almacenamiento de los certificados y host de acceso a la nube, si algo falla aqui no se establecera una conexion con la nube.
awshost = ""  # Endpoint
awsport = 8883  # Port no.
clientId = "raspberry"  # Thing_Name
thingName = "raspberry"  # Thing_Name
caPath = ""  # Root_CA_Certificate_Name
certPath = ""  # <Thing_Name>.cert.pem
keyPath = "/"  # <Thing_Name>.private.key

mqttc.tls_set(caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2,
              ciphers=None)  # pass parameters

mqttc.connect(awshost, awsport, keepalive=60)  # connect to aws server

mqttc.loop_start()  # Start the loop

# Se simulan posibles nombre o codigos de maquinas dentro del centro fabril.
# El algoritmo seleccionara una al azar para simular tomar medidas de varias maquinas
cod_maquinas = ['GUI6m', 'GUI3m', 'FREZ3m', 'CORPLASMA580', 'ARCSUM1800', 'CIL800mm']

# loop principal, no se rompera a menos que se cancele desde la consola.
while 1 == 1:
    sleep(5)
    # Este if verificara que exista conexion con la nube, si no lo hay se mostrara en pantalla el error.
    if connflag == True:
        cod_fabril = "CF004"  # suponemos el codigo de un centro fabril
        fecha = date.today().strftime("%d%m%Y")  # obtenemos la fecha del momento, y le damos el formato de ddmmaa
        hora = datetime.now().strftime("%H%M%S")  # obtenemos la hora del momento, y le damos el formato hhmmss
        clinea = random.randint(1,
                                8)  # La fundacion random.randint devuelve un numero entero aleatorio definido en el rango entregado
        index = random.randint(0, 5)
        cons = uniform(5.0,
                       25.0)  # consumo electrinico manquina simulado por una distribucion uniforme dentro del rango definido (5 y 25).
        cons = round(cons, 2)  # el numero obtenido es redondeado a 2 cifras
        # Aqui construimos la estructura del formato json, mediante una serie de variables.
        paymsg0 = "{"
        paymsg1 = "\"Cfabril\": \""
        paymsg2 = "\", \"Fecha\": \""
        paymsg3 = "\", \"Hora\": \""
        paymsg4 = "\", \"Clinea\": \""
        paymsg5 = "\", \"Cmaquina\": \""
        paymsg6 = "\", \"Consumo\": \""
        paymsg7 = "\" }"
        # se unen todas las variables y se guardan en una unica variable que representara el mensaje a enviar hacia la nube.
        paylodmsg = "{} {} {} {} {} {} {} {} {} {} {} {} {} {}".format(paymsg0, paymsg1, cod_fabril, paymsg2, fecha,
                                                                       paymsg3, hora, paymsg4, clinea, paymsg5,
                                                                       cod_maquinas[index], paymsg6, cons, paymsg7)
        paylodmsg = json.dumps(paylodmsg)
        paylodmsg_json = json.loads(paylodmsg)
        # el topic es el que debe ir tambien en la regla que se activara para almacenar los datos en Dynamodb
        mqttc.publish("raspberry", paylodmsg_json, qos=1)  # topic:  # Publishing data values
        print("msg sent: raspberry")  # Print sent info msg on console
        print(paylodmsg_json)
    else:
        print("esperando conexion...")           
