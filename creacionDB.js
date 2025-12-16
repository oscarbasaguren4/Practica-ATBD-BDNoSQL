
const DB_NAME = "bigdata_practica";
const COLL = "earthquakes";
const dbRef = db.getSiblingDB(DB_NAME);

if (dbRef.getCollectionNames().includes(COLL)) {
  dbRef[COLL].drop();
}

// Crear colección con validador JSON Schema
dbRef.createCollection(COLL, {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["magnitude", "parameters", "location", "datetime", "tsunami"],
      properties: {
        _id: { bsonType: "objectId" },
        magnitude: { bsonType: "decimal" },
        parameters: {
          bsonType: "object",
          required: ["cdi", "mmi", "sig", "nst", "dmin", "gap", "depth"],
          properties: {
            cdi: { bsonType: "int" },
            mmi: { bsonType: "int" },
            sig: { bsonType: "int" },
            nst: { bsonType: "int" },
            dmin: { bsonType: "decimal" },
            gap: { bsonType: "decimal" },
            depth: { bsonType: "decimal" }
          }
        },
        location: {
          bsonType: "object",
          required: ["latitude", "longitude"],
          properties: {
            latitude: { bsonType: "decimal" },
            longitude: { bsonType: "decimal" },
            country: { bsonType: "string" },
            seismic_zone: { bsonType: "string" }
          }
        },
        datetime: {
          bsonType: "object",
          required: ["year", "month"],
          properties: {
            year: { bsonType: "int" },
            month: { bsonType: "int" }
          }
        },
        tsunami: {
          bsonType: "object",
          required: ["risk"],
          properties: {
            risk: { bsonType: "bool" }
          }
        }
      }
    }
  },
  validationAction: "error"
});


print(`Base de datos '${DB_NAME}' creada correctamente.`);
print(`Colección '${COLL}' creada con el esquema especificado.`);
