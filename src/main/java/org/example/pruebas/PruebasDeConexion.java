package org.example.pruebas;

import com.mongodb.client.MongoClient;
import org.example.conectores.ConectorMongoDB;
import org.example.conectores.ConectorMySQL;
import org.example.conectores.ConectorNeo4j;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class PruebasDeConexion {
    public static void main(String[] args) throws SQLException {
        try {
            ConectorMySQL conexionMySQL = new ConectorMySQL();
            Connection con = conexionMySQL.Conexion();
            ResultSet resultadoCountry = conexionMySQL.leerCountry();
            ArrayList<String> country = new ArrayList<String>();
            int i = 1;
            while (resultadoCountry.next()) {
                System.out.println(resultadoCountry.getString(i++));
            }



            ConectorMongoDB conectorMongoDB = new ConectorMongoDB();
            conectorMongoDB.leer();

            ConectorNeo4j conectorNeo4j = new ConectorNeo4j();
            conectorNeo4j.pruebaConexion();

        } catch (Exception e) {
            System.out.println("Ocurrio un error al conectarse a una base de datos");
        }
    }
}
