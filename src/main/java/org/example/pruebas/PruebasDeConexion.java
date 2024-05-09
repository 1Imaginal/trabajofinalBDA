package org.example.pruebas;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDecimal128;
import org.bson.BsonInt32;
import org.bson.types.Decimal128;
import org.example.conectores.ConectorMongoDB;
import org.example.conectores.ConectorMySQL;
import org.example.conectores.ConectorNeo4j;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import org.bson.Document;
import org.neo4j.driver.Session;


public class PruebasDeConexion {
    public static void main(String[] args) throws SQLException {
        ConectorNeo4j conectorNeo4j = null;
        try {
            ConectorMySQL conexionMySQL = new ConectorMySQL();
            Connection con = conexionMySQL.Conexion();
            ResultSet resultadoCountry = conexionMySQL.leerCountry();
            ResultSetMetaData metaDataCountry = resultadoCountry.getMetaData();
            ArrayList<HashMap> paises = new ArrayList<>();

            int columnsNumber = metaDataCountry.getColumnCount();
            while (resultadoCountry.next()) {
                HashMap<String, Object> country = new HashMap<>();
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnName = metaDataCountry.getColumnName(i);
                    Object columnValue = resultadoCountry.getObject(i);
                    if (columnValue == null) {
                        columnValue = getDefaultColumnValue(columnName, metaDataCountry, i);
                    }
                    country.put(columnName, columnValue);
                }
                paises.add(country);
            }

            Gson gson = new Gson();
            String jsonArray = gson.toJson(paises);
            System.out.println(jsonArray);

            ConectorMongoDB conectorMongoDB = new ConectorMongoDB();
            conectorMongoDB.mongoClient.getDatabase("world").getCollection("country").drop();

            List<Document> documents = new ArrayList<>();
            for (HashMap<String, Object> hashMap : paises) {
                Document document = new Document(hashMap);
                documents.add(document);
            }
            conectorMongoDB.mongoClient.getDatabase("world").getCollection("country").insertMany(documents);
            System.out.println("Documentos insertados en la base de datos");

            HashMap<String, Document> paisesMongoDB = new HashMap<>();

            FindIterable<Document> iterable = conectorMongoDB.mongoClient.getDatabase("world").getCollection("country").find();
            MongoCursor<Document> cursor = iterable.iterator();

            while (cursor.hasNext()) {
                Document document = cursor.next();
                String id = document.getObjectId("_id").toString();
                paisesMongoDB.put(id, document);
            }

            conectorNeo4j = new ConectorNeo4j();
            conectorNeo4j.pruebaConexion();

            cursor = iterable.iterator();



            while (cursor.hasNext()) {
                Document document = cursor.next();
                try (Session session = conectorNeo4j.getDriver().session()) {
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("id", document.getObjectId("_id").toString());
                    parameters.put("code", document.getString("Code"));
                    parameters.put("name", document.getString("Name"));
                    parameters.put("localName", document.getString("LocalName"));
                    parameters.put("continent", document.getString("Continent"));
                    parameters.put("indepYear", document.getInteger("IndepYear"));
                    parameters.put("gnpOld", document.toBsonDocument().getDecimal128("GNPOld", new BsonDecimal128(new Decimal128(BigDecimal.ZERO))).doubleValue());
                    parameters.put("lifeExpectancy", document.toBsonDocument().getDecimal128("LifeExpectancy", new BsonDecimal128(new Decimal128(0))).doubleValue());
                    parameters.put("gnp", document.toBsonDocument().getDecimal128("GNP", new BsonDecimal128(new Decimal128(0))).doubleValue());
                    parameters.put("headOfState", document.getString("HeadOfState"));
                    parameters.put("capital", document.toBsonDocument().getInt32("Capital", new BsonInt32(0)).doubleValue());
                    parameters.put("region", document.getString("Region"));
                    parameters.put("surfaceArea", document.toBsonDocument().getDecimal128("SurfaceArea", new BsonDecimal128(new Decimal128(0))).doubleValue());
                    parameters.put("population", document.toBsonDocument().getInt32("Population", new BsonInt32(0)).intValue());


                    session.run("CREATE (c:Country {id: $id, code: $code, name: $name, localName: $localName, " +
                            "continent: $continent, indepYear: $indepYear, gnpOld: $gnpOld, lifeExpectancy: $lifeExpectancy, " +
                            "gnp: $gnp, headOfState: $headOfState, capital: $capital, region: $region, surfaceArea: $surfaceArea, " +
                            "population: $population})", parameters);

                    session.run("MATCH (c1:Country {continent: $continent}), (c2:Country {continent: $continent, id: $id}) " +
                            "WHERE c1 <> c2 " +
                            "MERGE (c1)-[:SHARES_CONTINENT_WITH]->(c2)", parameters);
                    System.out.println("País guardado en Neo4j: " + document.getString("Name"));
                } catch (Exception e) {
                    System.out.println("Ocurrió un error al guardar el país en Neo4j: " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.out.println("Ocurrió un error: " + e.getMessage());
        }
    }
    private static Object getDefaultColumnValue(String columnName, ResultSetMetaData metaData, int columnIndex) throws SQLException {
        int columnType = metaData.getColumnType(columnIndex);
        switch (columnType) {
            case Types.DECIMAL:
                return new Decimal128(BigDecimal.ZERO);
            case Types.INTEGER:
                return 0;
            default:
                return null;
        }
    }
}
