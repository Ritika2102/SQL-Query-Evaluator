/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 *
 * @author Rakshit
 */
public class Bootstrapper {

    public static FileWriter opfile;
    public static PrintWriter bw;
    public static ArrayList<Join> fromsubsel = null; // find out subselects and in our case it is one
    public static int querynum = 0; // while dispalying queries
    public static HashMap<String, CreateTable> tables;
    public static HashMap<String, Schema> schemas;
    public static String path;
    public static HashMap<SubSelect, Set<String>> InList = null; // in with subselect more than  one so key value  set is the that unique key is stored
    public static ArrayList<SubSelect> subsel = null; 
    public static int initialschemasize = 0; 
    public static ArrayList<String> addedschema = new ArrayList<>();
    public static int addcount = 0;
    public static HashMap<String, String> aliasmap = null;
        public static HashMap<String, String> reversealiasmap = null; // I cant display the alias map name

    public static String joincol = "";  // lineitem the one which is greater
    public static String ojoincol = ""; // other with whom this joins
    public static String ojointab = "";

//    public static Schema[] schemalist;
}
