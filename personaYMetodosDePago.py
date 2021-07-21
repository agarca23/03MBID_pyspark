#!/usr/bin/python

import sys
from pyspark import SparkContext, SparkConf


def  dividirYTarjetaMas1500 (linea):
    array =[]
    nombre, metodo, importe = linea.split(";")
    importe = float(importe)
    if(metodo == "Tarjeta de crédito" and importe>1500):
        array.append((nombre, 1))
    return array


def  dividirYTarjetaMenos1500 (linea):
    array =[]
    nombre, metodo, importe = linea.split(";")
    importe = float(importe)
    if(metodo == "Tarjeta de crédito" and importe<1500):
        array.append((nombre, 1))
    return array


def obtenerSuma(val1, val2):
    return int(val1)+int(val2)

def formatearSalida(tupla):
	return tupla[0]+";"+str(tupla[1])

conf = SparkConf().setMaster("local").setAppName("personaYMetodosDePago")
sc = SparkContext(conf = conf)

entrada = sys.argv[1]
salida = sys.argv[2]
print(salida)


datosEntrada = sc.textFile(entrada)

gastoTarjetaMas1500 = datosEntrada.flatMap(dividirYTarjetaMas1500).reduceByKey(obtenerSuma).map(formatearSalida)
gastoTarjetaMas1500.saveAsTextFile(salida+"/comprasCreditoMayorDe1500")

gastoTarjetaMenos1500 = datosEntrada.flatMap(dividirYTarjetaMenos1500).reduceByKey(obtenerSuma).map(formatearSalida)
gastoTarjetaMenos1500.saveAsTextFile(salida+"/comprasCreditoMenorDe1500")

