#!/bin/bash

echo "Creando carpeta en HDFS..."
hadoop fs -mkdir -p /user/hadoop/input

echo "Descargando archivo desde S3..."
aws s3 cp s3://asjimenezm-lab-emr/data/data.csv .

echo "Subiendo archivo a HDFS..."
hadoop fs -put -f data.csv /user/hadoop/input/

echo "Instalando dependencias..."
sudo yum install -y python3-pip
pip3 install --user mrjob

echo "Ejecutando Job 1: Total monto por tipo de crédito..."
python3 mi_mapreduce.py -r hadoop hdfs:///user/hadoop/input/data.csv --task total_monto_credito > total_monto_credito.csv

echo "Ejecutando Job 2: Promedio de tasa por producto..."
python3 mi_mapreduce.py -r hadoop hdfs:///user/hadoop/input/data.csv --task prom_tasa_producto > prom_tasa_producto.csv

echo "Ejecutando Job 3: Total desembolsos por municipio..."
python3 mi_mapreduce.py -r hadoop hdfs:///user/hadoop/input/data.csv --task total_desemb_municipio > total_desemb_municipio.csv

echo "Ejecutando Job 4: Promedio de monto por rango..."
python3 mi_mapreduce.py -r hadoop hdfs:///user/hadoop/input/data.csv --task prom_monto_rango > prom_monto_rango.csv

echo "Subiendo resultados a S3..."
aws s3 cp total_monto_credito.csv s3://asjimenezm-lab-emr/output/
aws s3 cp prom_tasa_producto.csv s3://asjimenezm-lab-emr/output/
aws s3 cp total_desemb_municipio.csv s3://asjimenezm-lab-emr/output/
aws s3 cp prom_monto_rango.csv s3://asjimenezm-lab-emr/output/

echo "Todos los jobs se ejecutaron y los resultados fueron subidos a S3."
