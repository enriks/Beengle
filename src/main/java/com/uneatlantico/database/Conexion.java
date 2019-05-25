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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.tika.exception.TikaException;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteOpenMode;
/**
 *
 * @author manuel.coto
 */
public class Conexion{
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
             pstmt.close();
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
        pstmt.close();
           result.close();
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
        pstmt.close();
          result.close();
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
        pstmt.close();
            res.close();
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
        pstmt.close();
            res.close();
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
        pstmt.close();
            res.close();
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
                PreparedStatement pstmt = conn.prepareStatement("Select * from PalabraDocumento where idPalabra = "+IdPalabra(nombrePalabra)+" and idDocumento="+IdArchivo(nombreDoc)+"")){
         res = pstmt.executeQuery();
        if(res.next()){
            result=true;
        }
        pstmt.close();
            res.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
            
        
        return result;
    }
         
         public int  NumDocPalExist(String nombre)
         {
             int num = 0;
             try {
                 Connection conn = this.connect();
                 PreparedStatement psmt = conn.prepareStatement("Select count(PalabraDocumento.idPalabra)numero from PalabraDocumento, Palabra where PalabraDocumento.idPalabra=Palabra.idPalabra and Palabra.nombrePalabra='"+nombre+"'");
                 ResultSet res = psmt.executeQuery();
                 if(res.next()){
                     num = res.getInt("numero");
                 }
                 psmt.close();
            res.close();
             } catch (SQLException e) {
                 System.err.println(e.getMessage());
             }
             return num;
         }
         
         public int  NumDocPalExist(int id)
         {
             int num = 0;
             try {
                 Connection conn = this.connect();
                 PreparedStatement psmt = conn.prepareStatement("Select count(PalabraDocumento.idPalabra)numero from PalabraDocumento, Palabra where PalabraDocumento.idPalabra=Palabra.idPalabra and Palabra.idPalabra="+id+"");
                 ResultSet res = psmt.executeQuery();
                 if(res.next()){
                     num = res.getInt("numero");
                 }
                 psmt.close();
            res.close();
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
                result = QueryExecute("Insert into Palabra(nombrePalabra) values (?)", a);
                if(ArchivoExist(doc)){
                a= new ArrayList<>();
                a.add(String.valueOf(IdPalabra(nombre)));
                a.add(String.valueOf(IdArchivo(doc)));
                result=QueryExecute("Insert into PalabraDocumento(idPalabra,idDocumento) values (?,?)", a);
                
                }
                else result =false;
             }else{
                    if(ArchivoExist(doc)){
                a= new ArrayList<>();
                a.add(String.valueOf(IdPalabra(nombre)));
                a.add(String.valueOf(IdArchivo(doc)));
                result=QueryExecute("Insert into PalabraDocumento(idPalabra,idDocumento) values (?,?)", a);
                
                }
                else result =false;
                }
             return result;
         }
         public boolean InsertEstadisticasPalabra(String nombrePalabra,String nombreDocumento,double tf,double idf)
         {
             boolean result =false;
             if(PalabraInsideDocExist(nombrePalabra,nombreDocumento)){
             List<String> a = new ArrayList();
             a.add(String.valueOf(tf));
             a.add(String.valueOf(idf));
             a.add(String.valueOf(tf*idf));
             a.add(String.valueOf(IdRelacion(nombrePalabra, nombreDocumento)));
             result = QueryExecute("Insert into EstadisticasPalabras(tf,idf,tfidf,idRelacion) values (?,?,?,?)", a);
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
        pstmt.close();
            res.close();
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
        pstmt.close();
            res.close();
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
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement("Select * from Palabra where nombrePalabra = '"+nombre+"'")){
        ResultSet res = pstmt.executeQuery();
        if(res.next()){
            result= res.getInt("idPalabra");
        }
        pstmt.close();
            res.close();
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
                PreparedStatement pstmt = conn.prepareStatement("Select * from PalabraDocumento where idPalabra ="+IdPalabra(nombrePalabra)+" and idDocumento="+IdArchivo(nombreDoc))){
        ResultSet res = pstmt.executeQuery();   
        if(res.next()){
            result= res.getInt("idRelacion");
        }
        pstmt.close();
            res.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
             }
             return result;
         }
         
         /*
         Esta cosa es para saber el nombre con el id de lo que sea
         */
         public String NombrePalabraById(int id){
             String nombre ="";
             try(Connection conn = this.connect(); 
                     PreparedStatement psmt = conn.prepareStatement("Select * from Palabra where idPalabra = "+id)){
                 ResultSet res = psmt.executeQuery();
                 if(res.next()){
                     nombre=res.getString("nombrePalabra");
                 }
             }catch(SQLException e){
                 System.err.println(e.getMessage());
             }
             return nombre;
         }
         public String NombreCarpetaById(int id){
             String nombre ="";
             try(Connection conn = this.connect(); 
                     PreparedStatement psmt = conn.prepareStatement("Select * from Carpetas where idCarpeta = "+id)){
                 ResultSet res = psmt.executeQuery();
                 if(res.next()){
                     nombre=res.getString("nombre");
                 }
             }catch(SQLException e){
                 System.err.println(e.getMessage());
             }
             return nombre;
         }
         
         public String NombreArchivoById(int id){
             String nombre ="";
             try(Connection conn = this.connect(); 
                     PreparedStatement psmt = conn.prepareStatement("Select * from Archivos where idDoc = "+id)){
                 ResultSet res = psmt.executeQuery();
                 if(res.next()){
                     nombre=res.getString("nombreDoc");
                 }
             }catch(SQLException e){
                 System.err.println(e.getMessage());
             }
             return nombre;
         }
         //////borrar//////////////////////////////////////////////////////////////////
         public void borrarTodo(){
             List<String> a =  new ArrayList<String>();
             QueryExecute("delete from Archivos", a);
             QueryExecute("Delete from Carpetas", a);
             QueryExecute("delete from CarpChilds", a);
             QueryExecute("delete from DocCarp", a);
         }
         public List<List> Indexacion(List<List> datos) throws IOException, TikaException, SQLException{
             
             try(Connection conn=this.connect();
                     PreparedStatement pstmt = conn.prepareStatement("Select * from Archivos"); ResultSet res = pstmt.executeQuery()){
                 while(res.next()){
                    
                     int k =0;
                     if(k==0)
                     {
                         List<List> lista = anal_isis.Palabras(anal_isis.parseExample(new File(res.getString("rutaDoc"))));
                         datos.add(lista);
                         String nombredoc = res.getString("nombreDoc");
                             for(int j =0; j<lista.get(0).size();j++){
                                 if(InsertPalabra(String.valueOf(lista.get(0).get(j)), nombredoc)){
                                     System.out.println("Palabra: "+lista.get(0).get(j)+" repeticiones: "+lista.get(0).get(j));
                                                 }
                                 }
                             }
                         
                     }
                 
             }catch(SQLException e){
                 System.err.println(e.getMessage());
             }
             
             return datos;
         }
     
         
         public boolean CalculoPalabra(List<List> listaPalabras)
         {
             boolean resp = false;
             try {
                 Connection conn = this.connect();
                 ResultSet res;
                 try (PreparedStatement psmt = conn.prepareStatement("Select * from PalabraDocumento")) {
                     res = psmt.executeQuery();
                     while(res.next())
                     {
                        
                                 for(List<List> lista : listaPalabras)
                                 {
                                     for(int k =0;k<lista.get(1).size();k++){
                                         
                                         try {
                                             String palara =NombrePalabraById(res.getInt("idPalabra"));
                                             String doc =NombreArchivoById(res.getInt("idDocumeto"));
                                             if(lista.get(0).get(k).equals(palara))
                                                 if(InsertEstadisticasPalabra(palara, doc, ((int)lista.get(1).get(k)/lista.get(1).size()), (Math.log10(listaPalabras.size()/NumDocPalExist(palara))+1))){
               
                                                    System.out.println("Inserto la mierda");
                                                }
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
         
         public void cerrarConexion() throws SQLException{Connection conn =this.connect();conn.close();}
}
