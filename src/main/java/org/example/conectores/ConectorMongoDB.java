package org.example.conectores;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.Iterator;

public class ConectorMongoDB {
    public static MongoClient mongoClient;
    public static MongoDatabase database;
    public static MongoCollection<Document> collection;

    public ConectorMongoDB() {
        this.mongoClient = MongoClients.create("mongodb://localhost:27017");
        this.database = mongoClient.getDatabase("world");
        this.collection = database.getCollection("country");

    }
    public void leer() {
        if (collection != null){
            System.out.println("Conexion exitosa a MongoDB");
            FindIterable<Document> iterDoc = collection.find();
            Iterator it = iterDoc.iterator();
            while (it.hasNext()) {
                System.out.println(it.next());
            }
            System.out.println();
        } else {
            System.out.println("Ocurrio un error al conectarse a Mongo");
            return;
        }

    }



}