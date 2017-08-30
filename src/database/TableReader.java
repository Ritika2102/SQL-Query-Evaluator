/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;





public class TableReader {
    
    private BufferedReader bufread;
    private FileReader fr;
    private String path;
    
    public TableReader(String name)
    {
        path = name+".csv";
        initialize();
    }
    
    public void initialize()
    {
        try {
            fr = new FileReader(path);
            bufread = new BufferedReader(fr);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
          }
    }
    
    public String ReadTable()
    {
        try {
            String line = bufread.readLine();
            if(line == null)
                return null;
            else
            {
                return line;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
    
}
