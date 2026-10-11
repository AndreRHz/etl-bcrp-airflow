from datetime import datetime
import duckdb
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

SERIES = {
    "PN39040BQ": "precio_x_var",
    "PN39042BQ": "terminos_intercambio_var",
    "PN39043BQ": "volumen_x_var",
    "PN39045BQ": "valor_x_var",
}
URL_BASE = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{codigo}/json/2012-1/2026-2/esp"

def extraer_datos():
    resultado = {}
    for codigo, columna in SERIES.items():
        respuesta = requests.get(URL_BASE.format(codigo=codigo), timeout=30)
        respuesta.raise_for_status()
        periodos = respuesta.json()["periods"]
        print(codigo, "->", columna, ":", len(periodos), "periodos")
        resultado[columna] = periodos
    return resultado

def transformar_datos(ti):
    datos = ti.xcom_pull(task_ids="extraer")  # {columna: [periodos...]}

    filas_por_fecha = {}
    for columna, periodos in datos.items():
        for periodo in periodos:
            nombre = periodo["name"]
            trimestre = int(nombre[1])
            anio = 2000 + int(nombre.split(".")[1])
            mes = (trimestre - 1) * 3 + 1
            fecha = f"{anio}-{mes:02d}-01"

            try:
                valor = float(periodo["values"][0])
            except (ValueError, TypeError):
                valor = None

            fila = filas_por_fecha.setdefault(fecha, {"fecha": fecha})
            fila[columna] = valor

    filas = sorted(filas_por_fecha.values(), key=lambda f: f["fecha"])
    print("Primeras filas:", filas[:2])
    return filas

def cargar_datos(ti):
    filas = ti.xcom_pull(task_ids="transformar")

    con = duckdb.connect("/opt/airflow/data/bcrp.duckdb")
    con.execute("""
        CREATE TABLE IF NOT EXISTS comercio_exterior (
            fecha DATE PRIMARY KEY,
            precio_x_var DOUBLE,
            terminos_intercambio_var DOUBLE,
            volumen_x_var DOUBLE,
            valor_x_var DOUBLE
        )
    """)

    for fila in filas:
        con.execute(
            "INSERT OR REPLACE INTO comercio_exterior VALUES (CAST(? AS DATE), ?, ?, ?, ?)",
            [fila["fecha"], fila.get("precio_x_var"), fila.get("terminos_intercambio_var"),
             fila.get("volumen_x_var"), fila.get("valor_x_var")],
        )

    total = con.execute("SELECT COUNT(*) FROM comercio_exterior").fetchone()[0]
    print("Filas en comercio_exterior:", total)
    con.close()

with DAG(
    dag_id="etl_bcrp",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:
    t_extraer = PythonOperator(task_id="extraer", python_callable=extraer_datos)
    t_transformar = PythonOperator(task_id="transformar", python_callable=transformar_datos)
    t_cargar = PythonOperator(task_id="cargar", python_callable=cargar_datos)

    t_extraer >> t_transformar >> t_cargar