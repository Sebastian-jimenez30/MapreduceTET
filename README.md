# MAPREDUCETET - Procesamiento Distribuido con Hadoop

## Autores

* Carlos Alberto Mazo Gil
* Anderson Jiménez

## Descripción

Este proyecto implementa Hadoop para procesar datos financieros públicos provenientes de la Superintendencia Financiera de Colombia. Se utilizó el modelo **MapReduce**, desplegado en un clúster EMR de AWS, con almacenamiento intermedio en **HDFS** y almacenamiento persistente en **S3**. Los resultados son expuestos mediante una **API Flask** en una instancia EC2.

## Fuente de Datos

Los datos utilizados (`data.csv`) fueron descargados desde la Superintendencia Financiera de Colombia en el siguiente enlace:

[Reporte Superfinanciera](https://www.superfinanciera.gov.co/powerbi/reportes/536)

## Requisitos

```bash
pip install -r requirements.txt
```

## Flujo de Ejecución

Se puede ejecutar todo el proceso automáticamente con el script:

```bash
./run_all_jobs.sh
```

Este script realiza los siguientes pasos:

1. **Crea la carpeta en HDFS**:

   ```
   hadoop fs -mkdir -p /user/hadoop/input
   ```

2. **Descarga el archivo desde S3**:

   ```
   aws s3 cp s3://asjimenezm-lab-emr/data/data.csv .
   ```

3. **Sube el archivo a HDFS**:

   ```
   hadoop fs -put -f data.csv /user/hadoop/input/
   ```

4. **Instala dependencias y ejecuta los siguientes jobs MapReduce**:

   * Total monto por tipo de crédito
   * Promedio de tasa por producto
   * Total de desembolsos por municipio
   * Promedio de monto por rango

5. **Sube los resultados nuevamente a S3**:

   ```
   aws s3 cp "archivo_resultado" s3://asjimenezm-lab-emr/output/
   ```

## Jobs MapReduce Implementados

Cada tarea se ejecuta con la siguiente sintaxis:

```bash
python3 mi_mapreduce.py -r hadoop hdfs:///user/hadoop/input/data.csv --task "nombre_tarea"
```

### Tareas disponibles:

| Tarea                    | Descripción                                     | Resultado                    |
| ------------------------ | ----------------------------------------------- | ---------------------------- |
| `total_monto_credito`    | Suma de montos por tipo de crédito              | `total_monto_credito.csv`    |
| `prom_tasa_producto`     | Promedio de tasa de interés por producto        | `prom_tasa_producto.csv`     |
| `total_desemb_municipio` | Total de desembolsos agrupados por municipio    | `total_desemb_municipio.csv` |
| `prom_monto_rango`       | Promedio de monto agrupado por rangos definidos | `prom_monto_rango.csv`       |

## API de Resultados

Una vez ejecutado el flujo, los resultados pueden ser consultados mediante la API Flask desplegada en una instancia EC2 pública.

IP Pública: `http://3.218.34.95` (Aqui puede ver todos los endpoints)

Ejemplo de endpoint:

```bash
http://3.218.34.95/api/total_monto_credito
```

> Nota: profe si desea ver la api nos puede decir para activarle el laboratorio.

## Video sustentación

[Video](https://drive.google.com/file/d/1ac0j-Dtldohljng7cKV9K2EnL6b9UyKJ/view?usp=sharing)
