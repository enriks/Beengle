/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.uneatlantico.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
/**
 *
 * @author manuel.coto
 */
public class Conexion {
    private Connection connect(){
        Connection conn = null;
        String url= "jdbc:sqlite:src/main/resources/files/Index.db";
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return conn;
    }
    public boolean QueryExecute(String query, List<String> argumentos)
    {
        boolean result = false;
        try(Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(query)){
            for(int k =0;k<argumentos.size();k++){
                    pstmt.setString(k+1, argumentos.get(k));
                    
            }
            pstmt.executeUpdate();
            result=true;
        }catch(SQLException e){
            System.err.println(e.getMessage());
        }
        return result;
    }
    public ResultSet QueryGet(String Query,List<String> argumentos){
        ResultSet result = null;
        try(Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(Query)){
            for(int k =0;k<argumentos.size();k++){
                    pstmt.setString(k+1, argumentos.get(k));
                    
            }
            result = pstmt.executeQuery();
            
        }
        catch(SQLException e){
            System.err.println(e.getMessage());
        }
        return result;
    }
}
