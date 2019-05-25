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
            pstmt.execute();
           
            result=true;
             pstmt.close();
            conn.close();
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
         public boolean InsertPalabra(String nombre,String doc,int tf,int numTot)
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
                if(PalabraInsideDocExist(nombre, doc))
                {
                    a=new ArrayList();
                    a.add(String.valueOf((double)tf/numTot));
                    a.add(String.valueOf(IdRelacion(nombre, doc)));
                    result=QueryExecute("Insert into EstadisticasPalabras(tf,idRelacion) values (?,?)",a);
                }else result = false;
                }
                else result =false;
             }else{
                    if(ArchivoExist(doc)){
                a= new ArrayList<>();
                a.add(String.valueOf(IdPalabra(nombre)));
                a.add(String.valueOf(IdArchivo(doc)));
                result=QueryExecute("Insert into PalabraDocumento(idPalabra,idDocumento) values (?,?)", a);
                if(PalabraInsideDocExist(nombre, doc))
                {
                    a=new ArrayList();
                    a.add(String.valueOf((double)tf/numTot));
                    a.add(String.valueOf(IdRelacion(nombre, doc)));
                    result=QueryExecute("Insert into EstadisticasPalabras(tf,idRelacion) values (?,?)",a);
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
             List<String> a = new ArrayList();
             a.add(String.valueOf(idf));
             a.add(String.valueOf(tf*idf));
             a.add(String.valueOf(IdRelacion(nombrePalabra, nombreDocumento)));
             result = QueryExecute("update EstadisticasPalabras set idf = ?, tfidf =? where idRelacion = ?", a);
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
         public List<List> Indexacion(List<Thread> hilos,List<List> datos) throws IOException, TikaException, SQLException{
             List<List> todo = new ArrayList<>();
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
                     pstmt2.close();
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
                 pstmt.close();
            
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
                 Connection conn = this.connect();
                 PreparedStatement psmt = conn.prepareStatement("Select * from PalabraDocumento");
                 ResultSet res = psmt.executeQuery();
                 while(res.next())
                 {
                     PreparedStatement psmt2 = conn.prepareStatement("select Palabra.nombrePalabra nombrePalabra, Archivos.nombreDoc nombreDoc,EstadisticasPalabras.tf tf from Archivos,Palabra, PalabraDocumento, EstadisticasPalabras where EstadisticasPalabras.idRelacion = PalabraDocumento.idRelacion and PalabraDocumento.idPalabra=Palabra.idPalabra and PalabraDocumento.idDocumento=Archivos.idDoc and PalabraDocumento.idPalabra= "+res.getInt("idPalabra"));
                     ResultSet res2 = psmt2.executeQuery();
                     while(res2.next()){
                     for(List<List> lista : listaPalabras)
                     {
                         for(int k =0;k<lista.get(1).size();k++){
                         
                             try {
                                 String palara =res2.getString("nombrePalabra");
                                 if(lista.get(0).get(k).equals(palara))
                                     InsertEstadisticasPalabra(palara, res2.getString("nombreDoc"), ((int)lista.get(1).get(k)/lista.get(1).size()), (Math.log10(listaPalabras.size()/NumDocPalExist(palara))+1));
                                     } catch (SQLException ex) {
                                 System.err.println(ex.getMessage());
                             }
                            
                     }
                     }
                     }
                     psmt2.close();
                 }
                 psmt.close();
            
             } catch (SQLException e) {
                 System.err.println(e.getMessage());
             }
             return resp;
         }
}
