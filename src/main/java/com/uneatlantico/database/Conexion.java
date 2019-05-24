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
import java.util.ArrayList;
import java.util.List;
import com.uneatlantico.data.TikaAnalysis;
import java.io.File;
import java.io.IOException;
import org.apache.tika.exception.TikaException;
/**
 *
 * @author manuel.coto
 */
public class Conexion {
    private TikaAnalysis anal_isis = new TikaAnalysis();
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
    
    public boolean CarpetExist(String nombre)
    {
        ResultSet result = null;
        boolean resultado =false;
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Carpetas where nombre = '"+nombre+"'")){
         result = pstmt.executeQuery();
        if(result.next()){
            resultado=true;
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return resultado;
    }
    
     public boolean ArchivoExist(String nombre)
    {
        ResultSet result = null;
        boolean resultado =false;
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Archivos where nombreDoc = '"+nombre+"'")){
         result = pstmt.executeQuery();
        if(result.next()){
            resultado=true;
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return resultado;
    }
     
      public boolean CarpetIsRoot(String nombre)
    {
        ResultSet res = null;
        boolean result =false;
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Carpetas where nombre = '"+nombre+"' and raiz = 1")){
        res = pstmt.executeQuery();
        if(res.next()){
            result=true;
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return result;
    }
      
       public boolean DocumentInsideCarpetExist(String nombreDoc,String nombreCarpet)
    {
        ResultSet res = null;
        boolean result =false;
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from DocCarp where idDocum =(select idDoc from Archivos where nombreDoc = '"+nombreDoc+"') and idCarpeta =(Select idCarpeta from Carpetas where nombre ='"+nombreCarpet+"')")){
         res = pstmt.executeQuery();
        if(res.next()){
            result=true;
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return result;
    }
        public boolean PalabraExtist(String nombre)
    {
        ResultSet res = null;
        boolean result =false;
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Palabra where nombrePalabra = '"+nombre+"'")){
         res = pstmt.executeQuery();
        if(res.next()){
            result=true;
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return result;
    }
         public boolean PalabraInsideDocExist(String nombrePalabra,String nombreDoc)
    {
        ResultSet res = null;
        boolean result =false;
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from PalabraDocumento where idPalabra = (Select idPalabra from Palabra where nombrePalabra ='"+nombrePalabra+"') and idDocumento=(select idDoc from Archivos where nombreDoc = '"+nombreDoc+"')")){
         res = pstmt.executeQuery();
        if(res.next()){
            result=true;
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return result;
    }
         /**
          * esto siguiente es solo para ejecutar inserts en diferentes tablas de la base de datos
          */
         public boolean InsertCarpeta(String nombre,int raiz,String padre)
         {
             boolean result =false;
             List<String> a = new ArrayList();
             if(!CarpetExist(nombre)){
                if(raiz==1){
                   a.add(nombre);
                   a.add(String.valueOf(raiz));
                   result = QueryExecute("Insert into Carpetas(nombre,raiz) values (?,?)", a);
                }
                else
                {
                   a.add(nombre);
                   a.add(String.valueOf(raiz));
                   result = QueryExecute("Insert into Carpetas(nombre,raiz) values (?,?)", a);
                   if(CarpetExist(nombre)){
                   a=new ArrayList<>();
                   a.add(String.valueOf(IdCarpeta(padre)));
                   a.add(String.valueOf(IdCarpeta(nombre)));
                   result=QueryExecute("Insert into CarpChilds values(?,?)", a);
                   }else result=false;
                }
             }
             return result;
         }
         public boolean InsertArchivo(String nombre,String ruta,String padre)
         {
             boolean result =false;
             List<String> a = new ArrayList();
             if(!ArchivoExist(nombre)){
                a.add(nombre);
                a.add(ruta);
                result = QueryExecute("Insert into Archivos(nombreDoc,rutaDoc) values (?,?)", a);
                if(CarpetExist(padre)){
                a= new ArrayList<>();
                a.add(String.valueOf(IdCarpeta(padre)));
                a.add(String.valueOf(IdArchivo(nombre)));
                result=QueryExecute("Insert into DocCarp values(?,?)", a);
                }else result=false;
             }
             return result;
         }
         public boolean InsertPalabra(String nombre,String doc)
         {
             boolean result =false;
             List<String> a = new ArrayList();
                if(!PalabraExtist(nombre)){
                a.add(nombre);
                result = QueryExecute("Insert into Palabra(nombre) values (?)", a);
                if(ArchivoExist(doc)){
                a= new ArrayList<>();
                a.add(String.valueOf(IdPalabra(nombre)));
                a.add(String.valueOf(IdArchivo(doc)));
                result=QueryExecute("Insert into PalabraDocumento values(?,?)", a);
                }
                else result =false;
             }
             return result;
         }
         public boolean InsertEstadisticasPalabra(String nombrePalabra,String nombreDocumento,int tf,int idf)
         {
             boolean result =false;
             if(PalabraInsideDocExist(nombrePalabra,nombreDocumento)){
             List<String> a = new ArrayList();
             a.add(String.valueOf(tf));
             a.add(String.valueOf(idf));
             a.add(String.valueOf(tf*idf));
             a.add(String.valueOf(IdRelacion(nombrePalabra, nombreDocumento)));
             result = QueryExecute("Insert into EstadisticasPalabra(tf,idf,tf-idf,idRelacion) values (?,?,?,?)", a);
             }
             return result;
         }
         
         /**
          * Esto es para obtener los id de cualquier cosa con el nombre
          */
         public int IdCarpeta(String nombre)
         {
             int result =0;
             if(CarpetExist(nombre))
             {
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Carpetas where nombre = '"+nombre+"'")){
        ResultSet res = pstmt.executeQuery();
        if(res.next()){
            result= res.getInt("idCarpeta");
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
             }
             return result;
         }
         ///////////
         public int IdArchivo(String nombre)
         {
             int result =0;
             if(ArchivoExist(nombre))
             {
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Archivos where nombreDoc = '"+nombre+"'")){
        ResultSet res = pstmt.executeQuery();
        if(res.next()){
            result= res.getInt("idDoc");
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
             }
             return result;
         }
         //////////////
         public int IdPalabra(String nombre)
         {
             int result =0;
             if(PalabraExtist(nombre))
             {
                 List<String> a = new ArrayList();
        a.add("'"+nombre+"'");
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Palabra where nombrePalabra = '"+nombre+"'")){
        ResultSet res = pstmt.executeQuery();
        if(res.next()){
            result= res.getInt("idPalabras");
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
             }
             return result;
         }
         
         ///////////////
         
         public int IdRelacion(String nombrePalabra,String nombreDoc){
             int result =0;
             if(PalabraInsideDocExist(nombrePalabra,nombreDoc))
             {
                        
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from PalabraDocumento where idPalabra ="+nombrePalabra+" and idDocumento="+nombreDoc)){
        ResultSet res = pstmt.executeQuery();   
        if(res.next()){
            result= res.getInt("idRelacion");
        }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
             }
             return result;
         }
         
         //////borrar
         public void borrarTodo(){
             List<String> a =  new ArrayList<String>();
             QueryExecute("delete from Archivos", a);
             QueryExecute("Delete from Carpetas", a);
             QueryExecute("delete from CarpChilds", a);
             QueryExecute("delete from DocCarp", a);
         }
         public List<Thread> Indexacion(List<Thread> hilos) throws IOException, TikaException{
             try(Connection conn=this.connect();
                     PreparedStatement pstmt = conn.prepareStatement("Select * from Archivos")){
                 ResultSet res = pstmt.executeQuery();
                 while(res.next()){
                     PreparedStatement pstmt2 = conn.prepareStatement("Select * from PalabraDocumento where idDocumento = "+res.getInt("idDoc"));
                     ResultSet res2 = pstmt2.executeQuery();
                     int k =0;
                     while(res2.next()){
                         k++;
                     }
                     if(k==0)
                     {
                         List<List> lista = anal_isis.Palabras(anal_isis.parseExample(new File(res.getString("rutaDoc"))));
                         Runnable runnable = ()->{
                             System.out.println("Empezo el hilo");
                             for(int j =0; j<lista.get(0).size();j++){
                                 System.out.println("Palabra: "+lista.get(0).get(j)+" repeticiones: "+lista.get(1).get(j));
                                 
                             }
                         };
                         hilos.add(new Thread(runnable));
                     }
                 }
             }catch(SQLException e){
                 System.err.println(e.getMessage());
             }
             
             return hilos;
         }
}
