/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.junit.Test;
import static org.junit.Assert.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import com.uneatlantico.database.Conexion;
/**
 *
 * @author manuel.coto
 */
public class DatabaseTest {
    @Test
    public void DatabaseTestQuery() {
        Conexion n = new Conexion();
        
        assertTrue(n.QueryExecute("Select * from Archivos"));
        n.CerrarConexion();
    }
    @Test
    public void DatabaseTestCarpetIsRoot(){
        Conexion n = new Conexion();
        assertFalse(n.CarpetIsRoot("asdasdjkasjdkja"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestCarpetInsideCarpet(){
        Conexion n = new Conexion();
        assertFalse(n.CarpetInsideCarpetExist("nada", "nada"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestCarpetExist(){
        Conexion n = new Conexion();
        assertFalse(n.CarpetExist("asdasdjkasjdkja"));
        n.CerrarConexion();
    }
    @Test
    public void DatabaseTestCarpetaHasChilds(){
        Conexion n = new Conexion();
        assertFalse(n.HasChilds("asdasdjkasjdkja"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestCarpetHasFiles(){
        Conexion n = new Conexion();
        assertFalse(n.HasFiles("asdasdjkasjdkja"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestArchivoExist(){
        Conexion n = new Conexion();
        assertFalse(n.ArchivoExist("asdasdjkasjdkja"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestCerrarConexion(){
        Conexion n = new Conexion();
        n.CerrarConexion();
        assertEquals(n.gerConnState(),"cerrada");
    }
    
    @Test
    public void DatabaseTestDocumentInsideCarpetExist(){
        Conexion n = new Conexion();
        assertFalse(n.DocumentInsideCarpetExist("asdasdjkasjdkja","nada"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestGetIdPalabra(){
        Conexion n = new Conexion();
        assertEquals(0, n.IdPalabra("asdkjasdk"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestGetIdCarpeta(){
    	Conexion n = new Conexion();
        assertEquals(0, n.IdCarpeta("asdkjasdk"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestGetIdArchivo(){
    	Conexion n = new Conexion();
        assertEquals(0, n.IdArchivo("asdkjasdk"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestNumDocPalExist(){
    	Conexion n = new Conexion();
        assertEquals(0, n.NumDocPalExist("asdkjasdk"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestPalabraExist(){
    	Conexion n = new Conexion();
        assertFalse(n.PalabraExtist("alskdnaskln"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestPalabraInsideDocExist(){
    	Conexion n = new Conexion();
        assertFalse(n.PalabraInsideDocExist("alskdnaskln","admalskdm"));
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestPalDoc(){
    	Conexion n = new Conexion();
        assertEquals(0, n.PalabrasDoc("asdkjasdk").get(0).size());
        n.CerrarConexion();
    }
    
    @Test
    public void DatabaseTestAllinsideCarpet(){
    	Conexion n = new Conexion();
        assertEquals(0, n.AllInsideCarp("askdlaks").size());
        n.CerrarConexion();
    }
}
