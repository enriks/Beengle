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
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.tika.exception.TikaException;

/**
 *
 * @author manuel.coto
 */
public class Conexion{
    private TikaAnalysis anal_isis = new TikaAnalysis();
    private Connection conn = null;
    private Statement stmt= null;
    public Conexion(){
        String url= "jdbc:sqlite:src/main/resources/files/Index.db";
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection(url);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
    public boolean QueryExecute(String query)
    {
        boolean result = false;
        try{
            this.stmt = conn.createStatement();
            stmt.executeUpdate(query);
           
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
        try {
                this.stmt = conn.createStatement();
                
         result = stmt.executeQuery("Select * from Carpetas where nombre = '"+nombre+"'");
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
        try {
               this.stmt = conn.createStatement();
         result = stmt.executeQuery("Select * from Archivos where nombreDoc = '"+nombre+"'");
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
        try {
                this.stmt = conn.createStatement();
        res = stmt.executeQuery("Select * from Carpetas where nombre = '"+nombre+"' and raiz = 1");
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
        try {
                stmt = conn.createStatement();
         res = stmt.executeQuery("Select * from DocCarp where idDocum =(select idDoc from Archivos where nombreDoc = '"+nombreDoc+"') and idCarpeta =(Select idCarpeta from Carpetas where nombre ='"+nombreCarpet+"')");
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
        try {
                stmt = conn.createStatement();
         res = stmt.executeQuery("Select * from Palabra where nombrePalabra = '"+nombre+"'");
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
        try {
                stmt = conn.createStatement();
         res = stmt.executeQuery("Select * from PalabraDocumento where idPalabra = "+IdPalabra(nombrePalabra)+" and idDocumento="+IdArchivo(nombreDoc)+"");
        if(res.next()){
            result=true;
        }
            
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return result;
    }
         
         public int  NumDocPalExist(String nombre)
         {
             int num = 0;
             try {
                 
                 stmt = conn.createStatement();
                 ResultSet res = stmt.executeQuery("Select count(PalabraDocumento.idPalabra)numero from PalabraDocumento, Palabra where PalabraDocumento.idPalabra=Palabra.idPalabra and Palabra.nombrePalabra='"+nombre+"'");
                 if(res.next()){
                     num = res.getInt("numero");
                 }
            
             } catch (SQLException e) {
                 System.err.println(e.getMessage());
             }
             return num;
         }
         
         public int  NumDocPalExist(int id)
         {
             int num = 0;
             try {
                 
                 stmt = conn.createStatement();
                 ResultSet res = stmt.executeQuery("Select count(PalabraDocumento.idPalabra)numero from PalabraDocumento, Palabra where PalabraDocumento.idPalabra=Palabra.idPalabra and Palabra.idPalabra="+id+"");
                 if(res.next()){
                     num = res.getInt("numero");
                 }
            
             } catch (SQLException e) {
                 System.err.println(e.getMessage());
             }
             return num;
         }
         /**
          * esto siguiente es solo para ejecutar inserts en diferentes tablas de la base de datos
          */
         public boolean InsertCarpeta(String nombre,int raiz,String padre)
         {
             boolean result =false;
             if(!CarpetExist(nombre)){
                if(raiz==1){
                   result = QueryExecute("Insert into Carpetas(nombre,raiz) values ('"+nombre+"',"+raiz+")");
                }
                else
                {
                   result = QueryExecute("Insert into Carpetas(nombre,raiz) values ('"+nombre+"',"+raiz+")");
                   if(CarpetExist(nombre)){
                   result=QueryExecute("Insert into CarpChilds values("+IdCarpeta(padre)+","+IdCarpeta(nombre)+")");
                   }else result=false;
                }
             }
             return result;
         }
         public boolean InsertArchivo(String nombre,String ruta,String padre)
         {
             boolean result =false;
             if(!ArchivoExist(nombre)){
                result = QueryExecute("Insert into Archivos(nombreDoc,rutaDoc) values ('"+nombre+"','"+ruta+"')");
                if(CarpetExist(padre)){
                result=QueryExecute("Insert into DocCarp values("+IdCarpeta(padre)+","+IdArchivo(nombre)+")");
                }else result=false;
             }
             return result;
         }
         public boolean InsertPalabra(String nombre,String doc,int tf,int numTot)
         {
             boolean result =false;
                if(!PalabraExtist(nombre)){
                result = QueryExecute("Insert into Palabra(nombrePalabra) values ('"+nombre+"')");
                if(ArchivoExist(doc)){
                result=QueryExecute("Insert into PalabraDocumento(idPalabra,idDocumento) values ("+IdPalabra(nombre)+","+IdArchivo(doc)+")");
                if(PalabraInsideDocExist(nombre, doc))
                {
                    double tfs = (double)tf/(double)numTot;
                    result=QueryExecute("Insert into EstadisticasPalabras(tf,idRelacion) values ("+tfs+","+IdRelacion(nombre, doc)+")");
                }else result = false;
                }
                else result =false;
             }else{
                    if(ArchivoExist(doc)){
                result=QueryExecute("Insert into PalabraDocumento(idPalabra,idDocumento) values ("+IdPalabra(nombre)+","+IdArchivo(doc)+")");
                if(PalabraInsideDocExist(nombre, doc))
                {
                    double tfs = (double)tf/(double)numTot;
                    result=QueryExecute("Insert into EstadisticasPalabras(tf,idRelacion) values ("+tfs+","+IdRelacion(nombre, doc)+")");
                }else result = false;
                }
                else result =false;
                }
             return result;
         }
         public boolean InsertEstadisticasPalabra(String nombrePalabra,String nombreDocumento,double tf,double idf)
         {
             boolean result =false;
             if(PalabraInsideDocExist(nombrePalabra,nombreDocumento)){
             double tfidf=tf*idf;
             result = QueryExecute("update EstadisticasPalabras set idf = "+idf+", tfidf ="+tfidf+" where idRelacion = "+IdRelacion(nombrePalabra, nombreDocumento));
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
        try {
                stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("Select * from Carpetas where nombre = '"+nombre+"'");
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
        try {
                stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("Select * from Archivos where nombreDoc = '"+nombre+"'");
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
        a.add(nombre);
        try {
                stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("Select * from Palabra where nombrePalabra = '"+nombre+"'");
        if(res.next()){
            result= res.getInt("idPalabra");
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
                        
        try {
                stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("Select * from PalabraDocumento where idPalabra ="+IdPalabra(nombrePalabra)+" and idDocumento="+IdArchivo(nombreDoc));   
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
             QueryExecute("delete from Archivos");
             QueryExecute("Delete from Carpetas");
             QueryExecute("delete from CarpChilds");
             QueryExecute("delete from DocCarp");
         }
         public List<List> Indexacion(List<Thread> hilos,List<List> datos) throws IOException, TikaException, SQLException{
             List<List> todo = new ArrayList<>();
             try{
                     stmt = conn.createStatement();
                 ResultSet res = stmt.executeQuery("Select * from Archivos");
                 while(res.next()){
                     Statement stmt2 = conn.createStatement();
                     ResultSet res2 = stmt2.executeQuery("Select * from PalabraDocumento where idDocumento = "+res.getInt("idDoc"));
                     int k =0;
                     while(res2.next()){
                         k++;
                     }
                     stmt2.close();
                     if(k==0)
                     {
                         List<List> lista = anal_isis.Palabras(anal_isis.parseExample(new File(res.getString("rutaDoc"))));
                         datos.add(lista);
                         String nombredoc = res.getString("nombreDoc");
                         Runnable runnable = ()->{
                             for(int j =0; j<lista.get(0).size();j++){
                                 manejoPalabras(lista, lista.get(0).get(j).toString(), nombredoc, (int)lista.get(1).get(j),j);
                             }
                         };
                         
                         hilos.add(new Thread(runnable));
                     }
                 }
            
             }catch(SQLException e){
                 System.err.println(e.getMessage());
             }
             todo.add(hilos);
             todo.add(datos);
             
             return todo;
         }
         private boolean manejoPalabras(List<List> lista,String Palabra,String doc,int tf,int j){
             boolean resp=false;
              System.out.println("Empezo el hilo");
                            
                                
                                     if(InsertPalabra(Palabra, doc,tf, lista.get(0).size())){
                                         resp=true;
                                         System.out.println("Palabra: "+lista.get(0).get(j)+" repeticiones: "+tf);
                                                 }
                                 
                             
             return resp;
         }
         
         public boolean CalculoPalabra(List<List> listaPalabras)
         {
             boolean resp = false;
             try {
                 
                 stmt = conn.createStatement();
                 ResultSet res = stmt.executeQuery("Select * from PalabraDocumento");
                 while(res.next())
                 {
                     Statement stmt2 = conn.createStatement();
                     ResultSet res2 = stmt2.executeQuery("select Palabra.nombrePalabra nombrePalabra, Archivos.nombreDoc nombreDoc,EstadisticasPalabras.tf tf from Archivos,Palabra, PalabraDocumento, EstadisticasPalabras where EstadisticasPalabras.idRelacion = PalabraDocumento.idRelacion and PalabraDocumento.idPalabra=Palabra.idPalabra and PalabraDocumento.idDocumento=Archivos.idDoc and PalabraDocumento.idPalabra= "+res.getInt("idPalabra"));
                     while(res2.next()){
                     for(List<List> lista : listaPalabras)
                     {
                         for(int k =0;k<lista.get(1).size();k++){
                         
                             try {
                                 String palara =res2.getString("nombrePalabra");
                                 if(lista.get(0).get(k).equals(palara))
                                     InsertEstadisticasPalabra(palara, res2.getString("nombreDoc"), ((double)((int)lista.get(1).get(k))/(double)lista.get(1).size()),(double) (Math.log10(listaPalabras.size()/NumDocPalExist(palara))+1));
                                     } catch (SQLException ex) {
                                 System.err.println(ex.getMessage());
                             }
                            
                     }
                     }
                     }
                 }
            
             } catch (SQLException e) {
                 System.err.println(e.getMessage());
             }
             return resp;
         }
         public void CerrarConexion(){
             try {
                 conn.close();
             } catch (Exception e) {
             }
         }
}
