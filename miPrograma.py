#!/usr/bin/python
import sys
from pyspark import SparkContext, SparkConf
'''
Programa creado por Jesus Moran
Este programa cuenta el numero de apariciones de cada palabra
'''
#inicializacion
conf = SparkConf().setMaster("local").setAppName("mi programa")
sc = SparkContext(conf = conf)
entrada = sys.argv[1]
salida = sys.argv[2]
print(salida)
#cargamos los datos de entrada
datosEntrada = sc.textFile(entrada)
#hacemos el conteo de cada palabra
conteo = datosEntrada.flatMap(lambda linea: linea.split(" ")).map(lambda palabra: (palabra, 1)).reduceByKey(lambda x, y: x + y)
#guardamos la salida
conteo.saveAsTextFile(salida)