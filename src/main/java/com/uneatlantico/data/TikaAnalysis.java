/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.uneatlantico.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.tika.exception.TikaException;
import org.apache.tika.*;

/**
 *
 * @author manuel.coto
 */
public class TikaAnalysis {
    public String parseExample(File file) throws IOException, TikaException{
       Tika tika = new Tika();
       return tika.parseToString(file);
   }
    public List<List> Palabras(String texto){
        List<List> lista = new ArrayList<>();
        List<String> listaString = new ArrayList<>();
        List<Integer> listaNum = new ArrayList<>();
        String[] cortado = texto.split("[^a-zA-Z0-9_á-úä-üÁ-ÚÄ-Ü]");
        for(String palabra : cortado ){
            if(!palabra.isEmpty()){
            if(contienelista(palabra,listaString)){
                
                        int index = listaString.indexOf(palabra);
                        listaNum.set(index, listaNum.get(index)+1);
                    
            } else {
                listaString.add(palabra);
                listaNum.add(1);
            }
        }}
        lista.add(listaString);
        lista.add(listaNum);
        return lista;
    }
    private boolean contienelista(String comp,List<String> lista){
         boolean resp = false;
        for(int k =0;k<lista.size();k++){
            if(comp.equals(lista.get(k))){
                resp=true;
            break;
            }
        }
        return resp;
    }
}
