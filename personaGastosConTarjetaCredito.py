#!/usr/bin/python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("mi programa")
sc = SparkContext(conf = conf)

entrada = sys.argv[1]
salida = sys.argv[2]
print(salida)


datosEntrada = sc.textFile(entrada)




def  dividirYTarjeta (linea):
    nombre, metodo, importe = linea.split(";")
    if(metodo == "Tarjeta de crédito"):
        return nombre, importe



gastoTarjeta = datosEntrada.flatMap(dividirYTarjeta).reduceByKey(lambda x, y: x+y)
gastoTarjeta.saveAsTextFile(salida)
