from flask import Flask, jsonify, abort
import pandas as pd
import boto3
from io import StringIO

app = Flask(__name__)

# Nombre del bucket S3
BUCKET = "asjimenezm-lab-emr"
FOLDER = "output"  # Carpeta dentro del bucket

# Cliente S3
s3 = boto3.client("s3")

# Archivos disponibles y rutas
AVAILABLE_ENDPOINTS = {
    "total_monto_credito": "total_monto_credito.csv",
    "prom_tasa_producto": "prom_tasa_producto.csv",
    "total_desemb_municipio": "total_desemb_municipio.csv",
    "prom_monto_rango": "prom_monto_rango.csv"
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

    key = f"{FOLDER}/{AVAILABLE_ENDPOINTS[nombre]}"

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")), sep="\t", header=None, names=["clave", "valor"])        
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        abort(500, description=f"Error al acceder al archivo: {str(e)}")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
