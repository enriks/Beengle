/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.uneatlantico.data;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author RIGO
 */
public class Configuration {
    private static final String PROPERTIES = "./file.properties";
    private static Properties property=null;
    private static final String PATH = "PATH";
    public String getDir() {
	if(property==null) 
            LoadProperties();
	return property.getProperty(PATH);
    }
    private static void LoadProperties() {
	try {
            //BasicConfigurator.configure();
            //log.info("Loading properties file"+PROPERTIES);
            property=new Properties();
            property.load(new FileInputStream(PROPERTIES));
            }catch(FileNotFoundException e) {
		e.printStackTrace();
            }catch(IOException e) {
		e.printStackTrace();
            }
	}
}
