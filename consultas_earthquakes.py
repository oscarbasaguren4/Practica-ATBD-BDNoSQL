from pymongo import MongoClient
from tabulate import tabulate
import time
import pprint

MONGO_URI = "mongodb://root:12345@localhost:27017/admin"
DB_NAME = "bigdata_practica"
COLLECTION_NAME = "earthquakes"

# Fichero donde se guardará toda la salida
OUTPUT_FILE = "resultados_earthquakes.txt"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# País concreto para las consultas B-i y B-ii
PAIS_CONCRETO = "Japan"  # cámbialo si quieres

consultas = {
    # A-i) Año del evento mayor de 2015
    "A-i) Terremotos con año > 2015": {
        "datetime.year": {"$gt": 2015},
    },

    # A-ii) País que empiece por “Japa”
    "A-ii) Terremotos en países que empiezan por 'Japa'": {
        "location.country": {"$regex": "^Japa", "$options": "i"},
    },

    # B-i) País concreto, magnitud > 7.0
    "B-i) País concreto con magnitud > 7.0": {
        "location.country": PAIS_CONCRETO,
        "magnitude": {"$gt": 7.0},
    },

    # B-ii) País concreto, riesgo de tsunami
    "B-ii) País concreto con riesgo de tsunami": {
        "location.country": PAIS_CONCRETO,
        "tsunami.risk": True,
    },
}

resultados = []
detalles_resultados = {}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:

    def log(msg=""):
        """Imprime en terminal y guarda también en el fichero de salida."""
        print(msg)
        f.write(str(msg) + "\n")

    log("\nINICIO DE EJECUCIÓN DE CONSULTAS SOBRE 'bigdata_practica.earthquakes'\n")
    log("=" * 90)

    for nombre, filtro in consultas.items():
        log(f"\nEjecutando consulta: {nombre}")
        inicio = time.time()
        resultados_cursor = list(collection.find(filtro, {"_id": 0}))
        fin = time.time()
        tiempo_ms = (fin - inicio) * 1000  # en milisegundos

        resultados.append([nombre, round(tiempo_ms, 2), len(resultados_cursor)])
        detalles_resultados[nombre] = resultados_cursor

        log(f"   Documentos devueltos: {len(resultados_cursor)} | Tiempo: {round(tiempo_ms, 2)} ms")

    log("\n" + "=" * 90)

    log("\nTABLA RESUMEN DE TIEMPOS DE EJECUCIÓN:\n")
    tabla = tabulate(resultados, headers=["Consulta", "Tiempo (ms)", "Documentos devueltos"], tablefmt="fancy_grid")
    log(tabla)

    log("\nRESULTADOS DETALLADOS DE LAS CONSULTAS:\n")
    for nombre, docs in detalles_resultados.items():
        log("\n" + "-" * 90)
        log(nombre)
        log("-" * 90)
        if docs:
            for doc in docs:
                texto_doc = pprint.pformat(doc, sort_dicts=False)
                log(texto_doc)
                log("-" * 60)
        else:
            log("No se encontraron resultados para esta consulta.")
        log("-" * 90)

    #  
    log(f"\nSalida completa guardada en: {OUTPUT_FILE}")

print(f"\n(Info) También se ha guardado en el archivo: {OUTPUT_FILE}")
