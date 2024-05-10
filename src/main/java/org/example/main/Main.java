package org.example.main;

import org.example.DAO.MongoDBDAO;
import org.example.DAO.MySQLDAO;
import org.example.DAO.Neo4jDAO;
import org.example.conectores.ConectorMongoDB;
import org.example.conectores.ConectorNeo4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;



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

        ArrayList<HashMap> ciudadesMongoDB = mongoDBDAO.transformarAHashMap(mongoDBDAO. leerDatos("city"));
        ArrayList<HashMap> paisesMongoDB = mongoDBDAO.transformarAHashMap(mongoDBDAO.leerDatos("country"));
        ArrayList<HashMap> idiomasMongoDB = mongoDBDAO.transformarAHashMap(mongoDBDAO.leerDatos("countrylanguage"));


        ConectorNeo4j conectorNeo4j = new ConectorNeo4j();
        conectorNeo4j.pruebaConexion();

        Neo4jDAO neo4jDAO = new Neo4jDAO();
        neo4jDAO.setSession(conectorNeo4j.getDriver().session());
        neo4jDAO.limpiarNeo4j();
        neo4jDAO.crearNodos("Paises", paisesMongoDB);
        neo4jDAO.crearNodos("IdiomasPaises", idiomasMongoDB);
        neo4jDAO.crearNodos("Ciudades", ciudadesMongoDB);
    }
}
