from flask import Flask, jsonify, abort
import pandas as pd
import boto3
from io import StringIO

app = Flask(__name__)

BUCKET = "asjimenezm-lab-emr"
FOLDER = "output"
s3 = boto3.client("s3")

AVAILABLE_ENDPOINTS = {
    "total_monto_credito": "total_monto_credito.csv",
    "prom_tasa_producto": "prom_tasa_producto.csv",
    "total_desemb_municipio": "total_desemb_municipio.csv",
    "prom_monto_rango": "prom_monto_rango.csv"
}

# Nombres personalizados por archivo
COLUMNS_BY_FILE = {
    "total_monto_credito.csv": ["Tipo_Credito", "Monto_Total"],
    "prom_tasa_producto.csv": ["Producto", "Tasa_Promedio"],
    "total_desemb_municipio.csv": ["Municipio", "Total_Desembolsos"],
    "prom_monto_rango.csv": ["Rango_Monto", "Monto_Promedio"]
}

@app.route("/")
def home():
    return {
        "message": "API de Resultados MapReduce (Flask + S3)",
        "endpoints": [f"/api/{k}" for k in AVAILABLE_ENDPOINTS]
    }

@app.route("/api/<nombre>")
def obtener_resultado(nombre):
    if nombre not in AVAILABLE_ENDPOINTS:
        abort(404, description="Análisis no disponible")

    filename = AVAILABLE_ENDPOINTS[nombre]
    key = f"{FOLDER}/{filename}"
    columns = COLUMNS_BY_FILE[filename]

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")), sep="\t", header=None, names=columns)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        abort(500, description=f"Error al acceder al archivo: {str(e)}")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
