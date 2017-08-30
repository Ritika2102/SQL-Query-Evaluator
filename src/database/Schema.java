/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.util.HashMap;
import net.sf.jsqlparser.statement.create.table.ColDataType;

/**
 *
 * @author Rakshit
 */
class Schema {

    public Schema() {
    schema = new HashMap<>();
    }
    
   public HashMap<String,colType> schema;
}
class colType{

    public colType(int col,ColDataType typ) {
        COLID = col;
        type =typ;
        tabname = "";
    }
    public colType(int col,ColDataType typ,String tab) {
        COLID = col;
        type =typ;
        tabname = tab;
    }
    public String tabname;
    public int COLID;
    public ColDataType type;
}
