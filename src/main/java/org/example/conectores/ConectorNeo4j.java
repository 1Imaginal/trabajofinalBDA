package org.example.conectores;

import lombok.Getter;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.Neo4jException;

public class ConectorNeo4j {
    String dbUri;
    String dbUser;
    String dbPassword;

    @Getter
    Driver driver;
    public ConectorNeo4j(){

        this.dbUri = "neo4j://localhost:7687";
        this.dbUser = "neo4j";
        this.dbPassword = "pretend-torch-orange-stand-isotope-1733";
        this.driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword));
    }
    public void pruebaConexion(){
        try (var driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {
            driver.verifyConnectivity();
            System.out.println("Conexion exitosa a neo4j");
        } catch (Neo4jException e) {
            System.out.println("Ocurrio un error al conectarse a neo4j");
        }
    }
}