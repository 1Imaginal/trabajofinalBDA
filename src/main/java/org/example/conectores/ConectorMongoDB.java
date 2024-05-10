package org.example.conectores;
import com.mongodb.MongoException;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.Iterator;

public class ConectorMongoDB {
    public static MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");;
    public static MongoDatabase database;
    public static MongoCollection<Document> collection;

    public ConectorMongoDB() {


    }
    public void leer(String BaseDeDatos, String colleccion) {
        try {
            this.database = mongoClient.getDatabase(BaseDeDatos);
            this.collection = database.getCollection(colleccion);
            FindIterable<Document> iterDoc = collection.find();
            Iterator it = iterDoc.iterator();
            while (it.hasNext()) {
                System.out.println(it.next());
            }
            System.out.println();
        } catch (MongoException e) {
            System.out.println("Ocurrio un error con MongoDB");
        }
        System.out.println("Conexion exitosa a MongoDB");
    }
}