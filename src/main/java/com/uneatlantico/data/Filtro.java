/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.uneatlantico.data;

import java.io.File;
import java.io.FileFilter;

/**
 *
 * @author RIGO
 */
public class Filtro implements FileFilter{

    private final String[] okFileExtensions = new String[] {"doc", "xlsx","pdf","txt"};

    @Override
  public boolean accept(File file)
  {
    for (String extension : okFileExtensions)
    {
      if (file.getName().toLowerCase().endsWith(extension))
      {
        return true;
      }
      if(file.isDirectory())
          return true;
    }
    return false;
  }
    }
    

