package org.example.DAO;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.example.conectores.ConectorMongoDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class MongoDBDAO {
    private ConectorMongoDB conectorMongoDB;
    public MongoDBDAO() {
        this.conectorMongoDB = new ConectorMongoDB();
    }

    public void agregarDatos(String collection, ArrayList<HashMap> datos) {
        List<Document> documents = new ArrayList<>();
        for (HashMap<String, Object> hashMap : datos) {
            Document document = new Document(hashMap);
            documents.add(document);
        }
        conectorMongoDB.mongoClient.getDatabase("world").getCollection(collection).insertMany(documents);
        System.out.println("Documentos insertados en la base de datos");

    }

    public ArrayList<Document> leerDatos(String collection) {
        ArrayList<Document> datos = new ArrayList<>();
        FindIterable<Document> iterable = conectorMongoDB.mongoClient.getDatabase("world").getCollection(collection).find();
        MongoCursor<Document> cursor = iterable.iterator();

        while (cursor.hasNext()) {
            Document document = cursor.next();
            String id = document.getObjectId("_id").toString();
            datos.add(document);
        }
        return datos;
    }

    public ArrayList<HashMap> transformarAHashMap(ArrayList<Document> documentos) {
        ArrayList<HashMap> listaDocumentos = new ArrayList<>();

        // Iterar sobre los documentos
        for (Document document : documentos) {
            // Crear un nuevo HashMap para este documento
            HashMap<String, Object> mapaDocumento = new HashMap<>();
            // Obtener las claves (nombres de los campos) de este documento
            Set<String> claves = document.keySet();
            // Iterar sobre las claves y añadir los pares clave-valor al HashMap
            for (String clave : claves) {
                Object valor = document.get(clave);
                // Realizar conversiones necesarias
                if (valor instanceof Decimal128) {
                    // Convertir Decimal128 a Double
                    valor = ((Decimal128) valor).doubleValue();
                } else if (valor instanceof ObjectId) {
                    // Convertir ObjectId a String
                    valor = valor.toString();
                }
                // Agregar el par clave-valor al HashMap del documento
                mapaDocumento.put(clave, valor);
            }
            // Agregar el HashMap de este documento a la lista de documentos
            listaDocumentos.add(mapaDocumento);
        }
        return listaDocumentos;
    }
}
