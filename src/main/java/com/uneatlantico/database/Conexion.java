/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.uneatlantico.database;


import com.adobe.xmp.XMPDateTimeFactory;
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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import jdk.nashorn.internal.ir.LiteralNode;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tika.exception.TikaException;

/**
 *
 * @author manuel.coto
 */
public class Conexion{
    private TikaAnalysis anal_isis = new TikaAnalysis();
    private Connection conn = null;
    private Statement stmt= null;
    
    private  Logger Log = Logger.getLogger(this.getClass());
    public Conexion(){
        
        PropertyConfigurator.configure("src\\main\\resources\\files\\log4j.properties");
        String url= "jdbc:sqlite:src/main/resources/files/Index.db";
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection(url);
        } catch (Exception e) {
            Log.error(e.getMessage());
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
            Log.error(e.getMessage());
        }
        return result;
    }
    /**
     * Esta cosa es para sacar datos nada mas
     * @param raiz
     * @return 
     */
    
    public List<String> GetCarpetas()
    {
        List<String> carp = new ArrayList();
        try {
            stmt=conn.createStatement();
            ResultSet res = stmt.executeQuery("Select * from Carpetas");
            while(res.next())
            {
                carp.add(res.getString("nombre"));
            }
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        return carp;
    }
    
    public List<String> GetArchivos()
    {
        List<String> carp = new ArrayList();
        try {
            stmt=conn.createStatement();
            ResultSet res = stmt.executeQuery("Select * from Archivos");
            while(res.next())
            {
                carp.add(res.getString("nombreDoc"));
            }
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        return carp;
    }
    
    public DefaultTableModel GetDataPalabra(DefaultTableModel modelo,String palabra){
        try {
            
        this.stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("SELECT DISTINCT Archivos.nombreDoc nombreDoc, EstadisticasPalabras.tfidf tfidf from Palabra,PalabraDocumento,EstadisticasPalabras,Archivos where PalabraDocumento.idDocumento=Archivos.idDoc and EstadisticasPalabras.idRelacion=PalabraDocumento.idRelacion and PalabraDocumento.idPalabra=Palabra.idPalabra and PalabraDocumento.idPalabra= "+IdPalabra(palabra));
        while(res.next()){
            modelo.addRow( new Object[]{res.getString("nombreDoc"),res.getDouble("tfidf")});
        }
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        
        return modelo;
    }
    public List<String> getRaices(List<String> lsita){
        try {
            
        this.stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("Select * from Carpetas where raiz =1 ");
        while(res.next()){
            lsita.add(res.getString("nombre"));
        }
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        
        return lsita;
    }
    
    public DefaultMutableTreeNode getChilds(DefaultMutableTreeNode raiz,String nombre)
    {
        try {
            this.stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("Select Carpetas.nombre nombreCarpeta from Carpetas,CarpChilds where CarpChilds.idHijo=Carpetas.idCarpeta and CarpChilds.idPadre="+IdCarpeta(nombre));
            while(res.next())
            {
                DefaultMutableTreeNode carpeta = new DefaultMutableTreeNode(res.getString("nombreCarpeta"));
                
                if(HasChilds(res.getString("nombreCarpeta")))
                {
                    carpeta =getChilds(carpeta, res.getString("nombreCarpeta"));
                }
                if(HasFiles(res.getString("nombreCarpeta")))
                {
                    carpeta=getFiles(carpeta, res.getString("nombreCarpeta"));
                }
                raiz.add(carpeta);
            }
                
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        return raiz;
    }
    
    public DefaultMutableTreeNode getFiles(DefaultMutableTreeNode raiz,String nombreCarp){
       
        try {
            this.stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("Select Archivos.nombreDoc nombreDoc, Archivos.rutaDoc rutaDoc from Archivos,DocCarp where DocCarp.idDocum = Archivos.idDoc and DocCarp.idCarpeta ="+IdCarpeta(nombreCarp));
            while(res.next())
            {
                DefaultMutableTreeNode archivos = new DefaultMutableTreeNode(res.getString("nombreDoc"));
                raiz.add(archivos);
            }
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        return raiz;
    }
    
    public List<String> AllInsideCarp(String nombre)
    {
        List<String> todo = new ArrayList<String>();
        try {
            this.stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("Select Carpetas.nombre nombreCarp, Archivos.nombreDoc nombreDoc from Carpetas,Archivos,DocCarp where DocCarp.idCarpeta = Carpetas.idCarpeta and DocCarp.idDocum = Archivos.idDoc and Carpetas.idCarpeta ="+IdCarpeta(nombre));
                    while(res.next())
                    {
                        todo.add(res.getString("nombreCarp"));
                        todo.add(res.getString("nombreDoc"));
                    }
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        return todo;
    }
    /**
     * jaja si
     * @param nombre
     * @return 
     */
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
            Log.error(e.getMessage());
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
            Log.error(e.getMessage());
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
            Log.error(e.getMessage());
        }
            
        
        return result;
    }
      public boolean HasChilds(String nombre){
          boolean resul = false;
          try {
              this.stmt=conn.createStatement();
              ResultSet res = stmt.executeQuery("select * from CarpChilds where idPadre ="+IdCarpeta(nombre));
              if(res.next()){
              resul = true;
          }
          } catch (Exception e) {
              Log.error(e.getMessage());
          }
          return resul;
      }
      
       public boolean HasFiles(String nombre){
          boolean resul = false;
          try {
              this.stmt=conn.createStatement();
              ResultSet res = stmt.executeQuery("select * from DocCarp where idCarpeta ="+IdCarpeta(nombre));
              if(res.next()){
              resul = true;
          }
          } catch (Exception e) {
              Log.error(e.getMessage());
          }
          return resul;
      }
       public boolean CarpetInsideCarpetExist(String nombreCarpetF,String nombreCarpetS)
    {
        ResultSet res = null;
        boolean result =false;
        try {
                stmt = conn.createStatement();
         res = stmt.executeQuery("Select * from CarpChilds where idPadre = '"+IdCarpeta(nombreCarpetF)+" and idHijo ="+IdCarpeta(nombreCarpetS)+"");
        if(res.next()){
            result=true;
        }
            
        } catch (SQLException e) {
            Log.error(e.getMessage());
        }
            
        
        return result;
    }
       public boolean DocumentInsideCarpetExist(String nombreDoc,String nombreCarpet)
    {
        ResultSet res = null;
        boolean result =false;
        try {
                stmt = conn.createStatement();
         res = stmt.executeQuery("Select * from DocCarp where idDocum ="+IdArchivo(nombreDoc)+" and idCarpeta ="+IdCarpeta(nombreCarpet)+"");
        if(res.next()){
            result=true;
        }
            
        } catch (SQLException e) {
            Log.error(e.getMessage());
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
            Log.error(e.getMessage());
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
            Log.error(e.getMessage());
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
                 Log.error(e.getMessage());
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
                 Log.error(e.getMessage());
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
         
          public boolean ReInsertEstadisticasPalabra(String nombrePalabra,String nombreDocumento,double tf,double idf)
         {
             boolean result =false;
             if(PalabraInsideDocExist(nombrePalabra,nombreDocumento)){
             double tfidf=tf*idf;
             result = QueryExecute("update EstadisticasPalabras set tf ="+tf+", idf = "+idf+", tfidf ="+tfidf+" where idRelacion = "+IdRelacion(nombrePalabra, nombreDocumento));
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
            Log.error(e.getMessage());
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
            Log.error(e.getMessage());
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
            Log.error(e.getMessage());
        }
             }
             return result;
         }
         ////////////////
         
         public List<List> PalabrasDoc(String nombre)
         {  List<String> palabra = new ArrayList();
            List<Double> tf = new ArrayList();
            List<Double> idf = new ArrayList();
                 List<List> todo = new ArrayList<>();
                 try {
                 this.stmt = conn.createStatement();
                 ResultSet res = stmt.executeQuery("SELECT Palabra.nombrePalabra nombrePalabra,EstadisticasPalabras.tf tf,EstadisticasPalabras.idf idf from Palabra,PalabraDocumento,EstadisticasPalabras where PalabraDocumento.idPalabra=Palabra.idPalabra and EstadisticasPalabras.idRelacion=PalabraDocumento.idRelacion and PalabraDocumento.idDocumento="+IdArchivo(nombre));
                 while(res.next())
                 {
                     palabra.add(res.getString("nombrePalabra"));
                     tf.add(res.getDouble("tf"));
                     idf.add(res.getDouble("idf"));
                 }
             } catch (Exception e) {
                 Log.error(e.getMessage());
             }
                 todo.add(palabra);
                 todo.add(tf);
                 todo.add(idf);
                 return todo;
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
            Log.error(e.getMessage());
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
             QueryExecute("delete from EstadisticasPalabras");
             QueryExecute("delete from Palabra");
             QueryExecute("delete from PalabraDocumento");
         }
         
         /**
          * Borrar Carpeta
          * @param nombre
          * @return 
          */
         public boolean BorrarTodoCarpeta(String nombre)
    {
        boolean result = false;
        try {
            if(CarpetIsRoot(nombre))
            {
                this.stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("Select Carpetas.nombre nombreCarpeta from Carpetas,CarpChilds where CarpChilds.idHijo=Carpetas.idCarpeta and CarpChilds.idPadre="+IdCarpeta(nombre));
            while(res.next())
            {
                if(HasChilds(nombre))
                    result =BorrarTodoCarpeta(res.getString("nombreCarpeta"));
                   
            }
            for(String item : AllInsideCarp(nombre)){
                         try {
                        if(CarpetExist(item)){
                                
                                result =QueryExecute("Delete from CarpChilds where idHijo="+IdCarpeta(item));
                                result =QueryExecute("Delete from Carpetas where idCarpeta="+IdCarpeta(item));
                                Log.debug("SE borro de la carpeta"+item);
                        }else
                        {
                            ResultSet res2 = stmt.executeQuery("select * from Archivos where nombreDoc='"+item+"'");
                            while(res2.next()){
                                Log.debug("Se borran los datos del archivo "+res2.getString("nombreDoc"));
                                ResultSet res3 = stmt.executeQuery("Select Palabra.nombrePalabra nombrePalabra, Palabra.idPalabra idPalabra from Palabra,PalabraDocumento where PalabraDocumento.idPalabra and PalabraDocumento.idDocumento="+res2.getInt("idDoc"));
                                while(res3.next()){
                                    Log.debug("SE esta borrando las palabras del documento "+item);
                                    result =QueryExecute("delete from EstadisticasPalabras where idRelacion ="+IdRelacion(res3.getString("nombrePalabra"), item));
                                    
                                    result =QueryExecute("delete from PalabraDocumento where idPalabra="+res3.getInt("idPalabra"));
                                    
                                    result =QueryExecute("delete from Palabra where idPalabra="+res3.getInt("idPalabra"));
                                }
                                
                                result =QueryExecute("delete from DocCarp where idDocum="+IdArchivo(item));
                                result =QueryExecute("delete from Archivos where idDoc="+IdArchivo(item));
                            }
                        }
                         } catch (SQLException ex) {
                                Log.error(ex.getMessage());
                            }
                            }
             Log.info("termino de borrar con resultado "+(result ? "bien":"mal"));
            }else
            for(String item : AllInsideCarp(nombre)){
                         try {
                        if(CarpetExist(item)){
                                
                                result =QueryExecute("Delete from CarpChilds where idHijo="+IdCarpeta(item));
                                result =QueryExecute("Delete from Carpetas where idCarpeta="+IdCarpeta(item));
                                Log.debug("SE borro de la carpeta"+item);
                        }else
                        {
                            ResultSet res2 = stmt.executeQuery("select * from Archivos where nombreDoc='"+item+"'");
                            while(res2.next()){
                                Log.debug("Se borran los datos del archivo "+res2.getString("nombreDoc"));
                                ResultSet res3 = stmt.executeQuery("Select Palabra.nombrePalabra nombrePalabra, Palabra.idPalabra idPalabra from Palabra,PalabraDocumento where PalabraDocumento.idPalabra and PalabraDocumento.idDocumento="+res2.getInt("idDoc"));
                                while(res3.next()){
                                    Log.debug("SE esta borrando las palabras del documento "+item);
                                    result =QueryExecute("delete from EstadisticasPalabras where idRelacion ="+IdRelacion(res3.getString("nombrePalabra"), item));
                                    
                                   result = QueryExecute("delete from PalabraDocumento where idPalabra="+res3.getInt("idPalabra"));
                                    
                                    result =QueryExecute("delete from Palabra where idPalabra="+res3.getInt("idPalabra"));
                                }
                                
                               result = QueryExecute("delete from DocCarp where idDocum="+IdArchivo(item));
                               result = QueryExecute("delete from Archivos where idDoc="+IdArchivo(item));
                            }
                        }
                         } catch (SQLException ex) {
                                Log.error(ex.getMessage());
                            }
                            }
             Log.info("termino de borrar con resltado "+(result ? "bien":"mal"));
        } catch (Exception e) {
            Log.error(e.getMessage());
        }
        return result;
    }
         
         /**
          * Borrar Archivo
          */
         public boolean BorrarTodoArchivo(String nombre){
             boolean result = false;
             try {
                 this.stmt = conn.createStatement();
                  ResultSet res2 = stmt.executeQuery("select * from Archivos where nombreDoc='"+nombre+"'");
                            while(res2.next()){
                                Log.debug("Se borran los datos del archivo "+res2.getString("nombreDoc"));
                                ResultSet res3 = stmt.executeQuery("Select Palabra.nombrePalabra nombrePalabra, Palabra.idPalabra idPalabra from Palabra,PalabraDocumento where PalabraDocumento.idPalabra and PalabraDocumento.idDocumento="+res2.getInt("idDoc"));
                                while(res3.next()){
                                    Log.debug("SE esta borrando las palabras del documento "+nombre);
                                    result =QueryExecute("delete from EstadisticasPalabras where idRelacion ="+IdRelacion(res3.getString("nombrePalabra"), nombre));
                                    
                                   result = QueryExecute("delete from PalabraDocumento where idPalabra="+res3.getInt("idPalabra"));
                                    
                                    result =QueryExecute("delete from Palabra where idPalabra="+res3.getInt("idPalabra"));
                                }
                                
                               result = QueryExecute("delete from DocCarp where idDocum="+IdArchivo(nombre));
                               result = QueryExecute("delete from Archivos where idDoc="+IdArchivo(nombre));
                            }
             } catch (Exception e) {
                 Log.error(e.getMessage());
             }
             return result;
         }
         /**
          * Funcinones que manejan palabras
          * @param datos
          * @return
          * @throws IOException
          * @throws TikaException
          * @throws SQLException 
          */
         public List<List> Indexacion(List<List> datos) throws IOException, TikaException, SQLException{
             List<List> todo = new ArrayList<>();
             List<Integer> fag = new ArrayList();
             try{
                     stmt = conn.createStatement();
                 ResultSet res = stmt.executeQuery("Select * from Archivos");
                 while(res.next()){
                    List<List> PalabrasDoc = PalabrasDoc(res.getString("nombreDoc"));
                     if(PalabrasDoc.get(0).isEmpty())
                     {
                         List<List> lista = anal_isis.Palabras(anal_isis.parseExample(new File(res.getString("rutaDoc"))));
                         datos.add(lista);
                         fag.add(0);
                         String nombredoc = res.getString("nombreDoc");
                             for(int j =0; j<lista.get(0).size();j++){
                                 manejoPalabras(lista, lista.get(0).get(j).toString(), nombredoc, (int)lista.get(1).get(j),j);
                             }
                         
                     }
                     else
                     {
                         List<List> lista = anal_isis.Palabras(anal_isis.parseExample(new File(res.getString("rutaDoc"))));
                         datos.add(lista);
                         fag.add(1);
                     }
                 }
            
             }catch(SQLException e){
                 Log.error(e.getMessage());
             }
             todo.add(datos);
             todo.add(fag);
             return todo;
         }
         private boolean manejoPalabras(List<List> lista,String Palabra,String doc,int tf,int j){
             boolean resp=false;
              Log.info("Empezo el hilo");
                            
                                
                                     if(InsertPalabra(Palabra, doc,tf, lista.get(0).size())){
                                         resp=true;
                                         Log.info("Palabra: "+lista.get(0).get(j)+" repeticiones: "+tf);
                                                 }
                                 
                             
             return resp;
         }
         
         public boolean CalculoPalabra(List<List> listaPalabras,List<Integer>flags)
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
                         int cunt = 0;
                        for(List<List> lista : listaPalabras)
                        {
                            for(int k =0;k<lista.get(1).size();k++){

                                try {
                                    String palara =res2.getString("nombrePalabra");
                                    if(lista.get(0).get(k).equals(palara))
                                        if(flags.get(cunt)==0)
                                        InsertEstadisticasPalabra(palara, res2.getString("nombreDoc"), ((double)((int)lista.get(1).get(k))/(double)lista.get(1).size()),(double) (Math.log10(listaPalabras.size()/NumDocPalExist(palara))+1));
                                    else
                                            InsertEstadisticasPalabra(palara, res2.getString("nombreDoc"), ((double)((int)lista.get(1).get(k))/(double)lista.get(1).size()),(double) (Math.log10(listaPalabras.size()/NumDocPalExist(palara))+1));
                                        } catch (SQLException ex) {
                                    Log.error(ex.getMessage());
                                }

                            }
                        }
                        cunt ++;
                     }
                 }
            
             } catch (SQLException e) {
                 Log.error(e.getMessage());
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
