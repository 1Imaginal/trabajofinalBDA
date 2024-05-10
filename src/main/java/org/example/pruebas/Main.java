package org.example.pruebas;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.example.DAO.MongoDBDAO;
import org.example.DAO.MySQLDAO;
import org.example.conectores.ConectorMongoDB;
import org.example.conectores.ConectorNeo4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

import org.bson.Document;
import org.neo4j.driver.Session;


public class Main {
    public static void main(String[] args) throws SQLException {
        MySQLDAO mySQLDAO = new MySQLDAO();
        ArrayList<HashMap> paises = mySQLDAO.leerDatos("country");
        ArrayList<HashMap> ciudades = mySQLDAO.leerDatos("city");
        ArrayList<HashMap> idiomas = mySQLDAO.leerDatos("countrylanguage");

        ConectorMongoDB conectorMongoDB = new ConectorMongoDB();
        conectorMongoDB.mongoClient.getDatabase("world").getCollection("city").drop();
        conectorMongoDB.mongoClient.getDatabase("world").getCollection("countryLanguague").drop();
        conectorMongoDB.mongoClient.getDatabase("world").getCollection("country").drop();

        MongoDBDAO mongoDBDAO = new MongoDBDAO();
        mongoDBDAO.agregarDatos("city", ciudades);
        mongoDBDAO.agregarDatos("country", paises);
        mongoDBDAO.agregarDatos("countrylanguage", idiomas);

        ArrayList<HashMap> ciudadesMongoDB = mongoDBDAO.transformarAHashMap(mongoDBDAO.leerDatos("city"));
        ArrayList<HashMap> paisesMongoDB = mongoDBDAO.transformarAHashMap(mongoDBDAO.leerDatos("country"));
        ArrayList<HashMap> idiomasMongoDB = mongoDBDAO.transformarAHashMap(mongoDBDAO.leerDatos("countrylanguages"));


        ConectorNeo4j conectorNeo4j = new ConectorNeo4j();
        conectorNeo4j.pruebaConexion();

        FindIterable<Document> iterable = conectorMongoDB.mongoClient.getDatabase("world").getCollection("country").find();
        MongoCursor<Document> cursor = iterable.iterator();

        Session session = conectorNeo4j.getDriver().session();
        try {
            session.run("MATCH (n) DETACH DELETE n");
        } catch (Exception e) {
            System.out.println("Error al eliminar la base de datos de neo4j");
        }

        while (cursor.hasNext()) {
            int i = 0;
            Document document = cursor.next();
            try {
                session.run("CREATE (c:Country {id: $_id, Code: $Code, Name: $Name, LocalName: $LocalName, " +
                        "Continent: $Continent, IndepYear: $IndepYear, GNPOld: $GNPOld, LifeExpectancy: $LifeExpectancy, " +
                        "GNP: $GNP, HeadOfState: $HeadOfState, Capital: $Capital, Region: $Region, SurfaceArea: $SurfaceArea, " +
                        "Population: $Population})", paisesMongoDB.get(i));

                session.run("MATCH (c1:Country {Continent: $Continent}), (c2:Country {Continent: $Continent, _id: $_id}) " +
                        "WHERE c1 <> c2 " +
                        "MERGE (c1)-[:SHARES_CONTINENT_WITH]->(c2)", paisesMongoDB.get(i));
                System.out.println("País guardado en Neo4j: " + document.getString("Name"));
                i++;
            } catch (Exception e) {
                System.out.println("Ocurrió un error al guardar el país en Neo4j: " + e.getMessage());
            }
        }
    }
}
