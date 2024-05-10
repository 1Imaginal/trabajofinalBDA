package org.example.DAO;

import org.bson.types.Decimal128;
import org.example.conectores.ConectorMySQL;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class MySQLDAO {
    private ConectorMySQL conectorMySQL;

    public MySQLDAO() {
        this.conectorMySQL = new ConectorMySQL();
    }

    public ArrayList<HashMap> leerDatos(String tabla) throws SQLException {
        try {
            Connection con = conectorMySQL.Conexion();
            ResultSet resultadoQuery = conectorMySQL.leer(tabla);
            ResultSetMetaData metaDataCountry = resultadoQuery.getMetaData();
            ArrayList<HashMap> datos = new ArrayList<>();

            int columnsNumber = metaDataCountry.getColumnCount();
            while (resultadoQuery.next()) {
                HashMap<String, Object> fila = new HashMap<>();
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnName = metaDataCountry.getColumnName(i);
                    Object columnValue = resultadoQuery.getObject(i);
                    if (columnValue == null) {
                        columnValue = getDefaultColumnValue(columnName, metaDataCountry, i);
                    }
                    fila.put(columnName, columnValue);
                }
                datos.add(fila);
            }
            return datos;
        } catch (SQLException e) {
            System.out.println("Ocurrio un error al leer los datos de MySQL " + e.getSQLState());
            return null;
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
