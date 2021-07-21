#!/usr/bin/python

import sys
from pyspark import SparkContext, SparkConf


def  dividirYTarjeta (linea):
    array =[]
    nombre, metodo, importe = linea.split(";")
    if(metodo == "Tarjeta de crédito"):
        array.append((nombre, importe))
    return array

def obtenerSuma(val1, val2):
    return float(val1)+float(val2)

def formatearSalida(tupla):
	return tupla[0]+";"+str(tupla[1])

conf = SparkConf().setMaster("local").setAppName("mi programa")
sc = SparkContext(conf = conf)

entrada = sys.argv[1]
salida = sys.argv[2]
print(salida)


datosEntrada = sc.textFile(entrada)

gastoTarjeta = datosEntrada.flatMap(dividirYTarjeta).reduceByKey(obtenerSuma).map(formatearSalida)
gastoTarjeta.saveAsTextFile(salida)
