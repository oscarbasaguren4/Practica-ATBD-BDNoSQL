import pandas as pd
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from geopy.geocoders import Nominatim
import time


MONGO_URI = "mongodb://root:12345@localhost:27017/admin"
DB_NAME = "bigdata_practica"
COLLECTION = "earthquakes"
USE_API = True


csv_file = "./data/earthquakes.csv"
df = pd.read_csv(csv_file)
df = df.head(100)

print(f" CSV cargado correctamente con {len(df)} registros.")
print(f" Columnas detectadas: {list(df.columns)}")


# Usa un user_agent con tu nombre, correo o proyecto (Nominatim lo exige)
geolocator = Nominatim(
    user_agent="BD_EARTHQUAKES",  # cámbialo si quieres
    timeout=10
)

# Cache local para evitar repetir consultas de la misma zona
cache_paises = {}

def obtener_pais_y_zona(lat, lon, i):
    """Obtiene el país y la zona sísmica de unas coordenadas"""
    coord_key = (round(lat, 2), round(lon, 2))
    if coord_key in cache_paises:
        return cache_paises[coord_key]

    if not USE_API:
        return "Desconocido", "Zona simulada"

    try:
        print(f"[{i}] Consultando país para ({lat}, {lon}) ...")
        location = geolocator.reverse((lat, lon), language="en", zoom=5, addressdetails=True)

        if location and "country" in location.raw.get("address", {}):
            country = location.raw["address"]["country"]
        else:
            country = "Desconocido"

        # Guarda en cache para reutilizar
        cache_paises[coord_key] = (country, "Zona de subducción" if 20 < abs(lat) < 40 else "Zona continental")
        return cache_paises[coord_key]

    except Exception as e:
        print(f"[{i}] Error geolocalizando: {e}")
        return "Desconocido", "Zona continental"


def preparar_registro(row, i):
    lat = float(row["latitude"])
    lon = float(row["longitude"])
    country, seismic_zone = obtener_pais_y_zona(lat, lon, i)

    return {
        "magnitude": Decimal128(str(row["magnitude"])),
        "parameters": {
            "cdi": int(row["cdi"]),
            "mmi": int(row["mmi"]),
            "sig": int(row["sig"]),
            "nst": int(row["nst"]),
            "dmin": Decimal128(str(row["dmin"])),
            "gap": Decimal128(str(row["gap"])),
            "depth": Decimal128(str(row["depth"]))
        },
        "location": {
            "latitude": Decimal128(str(lat)),
            "longitude": Decimal128(str(lon)),
            "country": country,
            "seismic_zone": seismic_zone
        },
        "datetime": {
            "year": int(row["Year"]),
            "month": int(row["Month"])
        },
        "tsunami": {
            "risk": bool(row["tsunami"])
        }
    }


client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

documentos = []

print("\n Procesando e insertando registros...\n")
for i, fila in df.iterrows():
    print(f" [{i+1}/{len(df)}] Procesando registro...")
    try:
        doc = preparar_registro(fila, i+1)
        documentos.append(doc)
        print(f"  [{i+1}] País={doc['location']['country']}, Zona={doc['location']['seismic_zone']}")
    except Exception as e:
        print(f" [{i+1}] Error preparando registro: {e}")
    if USE_API:
        time.sleep(1.2)  

print("\n Insertando en MongoDB...")
if documentos:
    result = collection.insert_many(documentos)
    print(f" Se insertaron {len(result.inserted_ids)} registros en '{DB_NAME}.{COLLECTION}' correctamente.")
else:
    print(" No se generaron documentos para insertar.")

client.close()
print("Proceso completado.")
