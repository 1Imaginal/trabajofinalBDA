package org.example.conectores;

import java.sql.*;

public class ConectorMySQL {
    Connection con;
    public Connection Conexion(){

        try {
            con = DriverManager.getConnection("jdbc:mysql://localhost/world?" +
                            "user=root&password=admin&serverTimezone=UTC");
            this.con = con;
            System.out.println("Conexion exitosa a MySQL");
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return con;
    }
    public ResultSet leer(String tabla) throws SQLException{
        Statement statement = con.createStatement();
        ResultSet resultado = statement.executeQuery("SELECT * FROM " + tabla);
        System.out.println("Return value is : " + resultado.toString() );
        return resultado;
    }

}