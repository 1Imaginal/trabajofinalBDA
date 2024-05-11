package org.example.DAO;

import lombok.Getter;
import lombok.Setter;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.NoSuchRecordException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Neo4jDAO {

    @Getter @Setter
    Session session;

    public void limpiarNeo4j() {
        session.run("MATCH(n) DETACH DELETE n");
    }
    public void crearNodos(String nombre, ArrayList<HashMap> datos) {

        try (Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "pretend-torch-orange-stand-isotope-1733"));
             Session session = driver.session()) {

            // Iterar sobre la lista de HashMaps
            for (HashMap<String, Object> nodos : datos) {
                // Construir la consulta Cypher dinámica para cada HashMap
                StringBuilder cypherQuery = new StringBuilder("CREATE (p:" + nombre + "{");
                for (Map.Entry<String, Object> entry : nodos.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    if (!key.equals(nombre)) {
                        cypherQuery.append(key).append(": $").append(key).append(", ");
                    }
                }
                cypherQuery.delete(cypherQuery.length() - 2, cypherQuery.length()); // Eliminar la última coma
                cypherQuery.append("})");

                // Ejecutar la consulta Cypher para crear el nodo
                session.writeTransaction(tx -> {
                    tx.run(cypherQuery.toString(), nodos);
                    return null;
                });
            }
            System.out.println(nombre + " creado");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void ejecutarConsultaCypher(String consulta) {
        try (Transaction tx = session.beginTransaction()) {
            tx.run(consulta);
            tx.commit();
        } catch (NoSuchRecordException e) {
            System.err.println("Error al ejecutar la consulta Cypher: " + e.getMessage());
        }
    }
}
