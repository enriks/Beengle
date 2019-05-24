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

/**
 *
 * @author manuel.coto
 */
public class DatabaseTest {
    /*@Test
    public void DatabaseTestQuery() {
        Conexion n = new Conexion();
        List<String> ss = new ArrayList<>();
        ss.add("nada");
        ss.add("nada");

        assertTrue(n.QueryExecute("insert into Archivos(nombreDoc,rutaDoc) values(?,?)", ss));
    }
    @Test
    public void DatabaseTestQueryTwo(){
        Conexion n = new Conexion();
        List<String> ss = new ArrayList<>();
        ResultSet something = n.QueryGet("select * from Archivos", ss);
        assertNotNull(something);
    }*/
}
