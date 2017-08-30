/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.io.BufferedReader;
import java.io.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

/**
 *
 * @author Rakshit
 */
public class Fetch {

    MappedByteBuffer CopyToMemory(String name) {
        File filetocopy = null;
        FileChannel fc = null;
        MappedByteBuffer buffer = null;
        try {
            filetocopy = new File(name);
            fc = new RandomAccessFile(filetocopy, "r").getChannel();
            buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            buffer.load();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                fc.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return buffer;
    }

    ArrayList<String> GetArrayOfStringsFromMemoryMap(MappedByteBuffer buffer) {
        ArrayList<String> result = new ArrayList<>();
        StringBuilder buildS = new StringBuilder();
        char item;
        for (int j = 0; j < buffer.limit(); j++) {
            if ((item = (char) buffer.get()) != '\r') {
                buildS = buildS.append(item);
            } else {
                result.add(buildS.toString());
                buildS = new StringBuilder();
            }
        }
        result.add(buildS.toString());
        return result;
    }

    ArrayList<jointuple> GetArrayOfStringsFromFile(String name) {
        String path = Bootstrapper.path + name + ".csv";
        FileReader fr = null;
        ArrayList<jointuple> result = new ArrayList<>();
        try {
            fr = new FileReader(path);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr);
        String temp;
        String naam = GetTableName(name);
        try {
            while ((temp = br.readLine()) != null) {
                jointuple jt = new jointuple();
                jt.jointup.put(naam, temp);
                result.add(jt);

            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    private static String GetTableName(String tabnam) {
        if (tabnam == null) {
            return Bootstrapper.aliasmap.get(tabnam);
        }
        if (!Bootstrapper.schemas.containsKey(tabnam.toLowerCase())) {
            return Bootstrapper.aliasmap.get(tabnam);
        } else {
            return tabnam.toLowerCase();
        }
    }

    ArrayList<String> GetArrayFromFile(String name) {
        String path = Bootstrapper.path + name + ".csv";
        FileReader fr = null;
        ArrayList<String> result = new ArrayList<>();
        try {
            fr = new FileReader(path);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        BufferedReader br = new BufferedReader(fr);
        String temp;

        try {
            while ((temp = br.readLine()) != null) {

                result.add(temp);

            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    ArrayList<jointuple> populatehash(HashMap<String, byte[]> inmemtable, joincols jc1, Expression whereex, List<Join> join) {
        byte del[] = null;
        try {
            del = "~".getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Fetch.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (join.size() == 1) {
            String tab = ((Table) join.get(0).getRightItem()).getName();
            String path = Bootstrapper.path + tab + ".csv";

            customeval cuseval = new customeval(Bootstrapper.aliasmap);

            FileReader fr = null;
            ArrayList<jointuple> result = new ArrayList<>();
            try {
                fr = new FileReader(path);
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            }
            BufferedReader br = new BufferedReader(fr);
            String temp;
            int c1 = 0;
            String naam = GetTableName(tab);
            try {
                while ((temp = br.readLine()) != null) {
                    c1++;
                    if (c1 % 500000 == 0) {
                        System.out.println("Fetched " + c1 + " tuples into memory from " + naam + "...");
                    }
                    jointuple jt = new jointuple();
                    jt.jointup.put(naam, temp);

                    cuseval.settuple(jt);
                    PrimitiveValue pv;
                    try {
                        pv = cuseval.eval(whereex);

                        if (pv.toBool()) {
                            result.add(jt);
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(Fetch.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return result;
        } else {
            String path = Bootstrapper.path + jc1.table + ".csv";
            String tabn = GetTableName(jc1.table);
            customeval cuseval = new customeval(Bootstrapper.aliasmap);

            FileReader fr = null;
            try {
                fr = new FileReader(path);
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            }
            BufferedReader br = new BufferedReader(fr);
            String temp;
            String naam = GetTableName(jc1.table);
            try {
                int c = 0;
                while ((temp = br.readLine()) != null) {
                    jointuple jt = new jointuple();
                    String[] tempa = temp.split("\\|");
                    jt.jointup.put(naam, temp);

                    cuseval.settuple(jt);
                    PrimitiveValue pv;
                    try {
                        pv = cuseval.eval(whereex);
                        c++;
                        if (c % 500000 == 0) {
                            System.out.println("Fetched " + c + " tuples into memory from " + naam + "...");
                        }
                        if (pv.toBool()) {

                            colType ckey = Bootstrapper.schemas.get(tabn).schema.get(jc1.column.toLowerCase());

                            String key = tempa[ckey.COLID];
                            if (key.length() > 6) {
                                key = key.substring(0, 6);
                            }
                            byte[] ntemp = temp.getBytes();
                            if (inmemtable.containsKey(key)) {
                                byte[] old = inmemtable.get(key);
                                byte[] newb = new byte[old.length + del.length + ntemp.length];
                                System.arraycopy(old, 0, newb, 0, old.length);
                                System.arraycopy(del, 0, newb, old.length, del.length);
                                System.arraycopy(ntemp, 0, newb, old.length + del.length, ntemp.length);
                                inmemtable.put(key, newb);
                            } else {
                                inmemtable.put(key, ntemp);
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(Fetch.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return new ArrayList<>();
        }
    }

}
