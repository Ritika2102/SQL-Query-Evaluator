/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.File;
import java.io.*;
import java.nio.MappedByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SubSelect;


public class Database {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        int i;
        Bootstrapper.tables = new HashMap<>();
        Bootstrapper.schemas = new HashMap<>();

//        File sqlfile = new File("schema.sql");
//        Bootstrapper.path = "";
        File sqlfile = new File(args[0]);
        Bootstrapper.path = args[2];
//        
//        Fetch f =new Fetch();
//        ArrayList<String> res = f.GetArrayFromFile("orders");
//        try {
//            Bootstrapper.opfile = new FileWriter(new File("output1.csv"));
//        } catch (IOException ex) {
//            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
//        }
//            Bootstrapper.bw = new PrintWriter(Bootstrapper.opfile);
//            Bootstrapper.bw.write("");
//            int c =0 ;
//        for (String re : res) {
//            if(c%3!=0)
//            {
//                Bootstrapper.bw.println(re);
//            }
//            c++;
//        }

        try {
            Bootstrapper.opfile = new FileWriter(new File("output.csv"));
            Bootstrapper.bw = new PrintWriter(Bootstrapper.opfile);
            Bootstrapper.bw.write("");
            FileReader stream = new FileReader(sqlfile);

            CCJSqlParser parser = new CCJSqlParser(stream);
            Statement statement;
            while ((statement = parser.Statement()) != null) {
                if (statement instanceof CreateTable) {
                    CreateTable ct = (CreateTable) statement;
                    Bootstrapper.tables.put(ct.getTable().getName(), ct);
                    Schema sc = new Schema();
                    int j = 0;
                    for (ColumnDefinition cd : ct.getColumnDefinitions()) {
                        sc.schema.put(cd.getColumnName().toLowerCase(), new colType(j, cd.getColDataType()));
                        j++;
                    }
                    Bootstrapper.schemas.put(ct.getTable().getName().toLowerCase(), sc);
                } else if (statement instanceof Select) {
                    Bootstrapper.InList = new HashMap<>();
                    OutputModel om = new OutputModel();
                    Bootstrapper.aliasmap = new HashMap<>();
                    Bootstrapper.reversealiasmap = new HashMap<>(); // why reversealiasmap
                    Bootstrapper.initialschemasize = Bootstrapper.schemas.size(); 
                    Bootstrapper.addcount = 0; // what count is for ???
                    consoleop("###########################################################################################################################################################");

                    SelectBody sb = ((Select) statement).getSelectBody();
                    if (sb instanceof Union) { // get all the lists
                        ArrayList<ArrayList<jointuple>> jtss = new ArrayList<ArrayList<jointuple>>();
                        for (PlainSelect pl : ((Union) sb).getPlainSelects()) { // didn't understand this line
                            ArrayList<jointuple> jts = ExecuteSelect(pl, om);
                            jtss.add(jts);
                        }
                        HashMap<String, jointuple> finallst = new HashMap<>();

                        for (ArrayList<jointuple> jts : jtss) { // is this condition if joins are also there
                            if (jts.isEmpty()) {
                                continue;
                            }
                            Object[] temp = jts.get(0).jointup.keySet().toArray(); // to remove the duplicates
                            String ex = String.valueOf(Bootstrapper.addcount);
                            for (jointuple jt : jts) {
                                String key = jt.jointup.get(temp[0]).split("\\|")[0];
                                if (!finallst.containsKey(key)) {
                                    finallst.put(key, jt);
                                }
                            }
                        }
                        Display(new ArrayList<jointuple>(new ArrayList<>(finallst.values())), om, statement);
                    } else {
                        ArrayList<jointuple> jts = ExecuteSelect(sb, om);
                        Display(jts, om, statement);
                    }
                    if (Bootstrapper.initialschemasize != Bootstrapper.schemas.size()) { // why we need condition of intial schema and schema
                        for (String schem : Bootstrapper.addedschema) {
                            Bootstrapper.schemas.remove(schem); // added schema but not needed later so delete
                        }
                        Bootstrapper.addedschema.clear();
                    }
                    Bootstrapper.subsel = null;
                    Bootstrapper.fromsubsel = null;
                } else {
                    consoleop("panic...");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        Bootstrapper.bw.close();

        System.out.println("###########################################################################################################################################################");

        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("The output can also be found in a file named output.csv in the execution folder");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("###########################################################################################################################################################");

    }

    private static void consoleop(String out) {
        Bootstrapper.bw.println(out);
        System.out.println(out);
    }

    private static void Display(ArrayList<jointuple> jts, OutputModel om, Statement statement) {
        consoleop("SQL query " + (++Bootstrapper.querynum));
        consoleop(statement.toString());
        consoleop("");
        DisplaySummary(om);

        for (jointuple jt : jts) {
            Object[] temp = jt.jointup.keySet().toArray();
            String sc = String.valueOf(temp[0]);
            String[] result = jt.jointup.get(sc).split("\\|");
            String out = "";
            for (String string : result) {
                out = out + string + ",";
            }
            out = out.substring(0, out.length() - 1);
            consoleop(out);
        }
    }

    private static ArrayList<jointuple> ExecuteSelect(SelectBody select, OutputModel om) {
        ArrayList<joincols> joininfo = new ArrayList<>(); // list of table names and colums to be joined

        //Simple select from jsqlparser
        PlainSelect pl = (PlainSelect) select;
        //The Tables to be joined or to be used
        List<Join> join = pl.getJoins();
        if (join == null) {
            join = new ArrayList<Join>();
        }
        FromItem ft = pl.getFromItem();

        Join subsel = null;
        HashMap<String, ArrayList<jointuple>> subresult = new HashMap<>(); // store result of from subselect
        ArrayList<Join> testj = new ArrayList<>();
        for (Join join1 : join) {
            testj.add(join1);
        }
        Join nju = new Join();
        nju.setRightItem(ft);
        testj.add(nju);
        if (GetSubSelectPresent(testj)) {
            for (Join subl : Bootstrapper.fromsubsel) {
                subresult.put(subl.getRightItem().getAlias(), ExecuteSelect(((SubSelect) subl.getRightItem()).getSelectBody(), om));
                Bootstrapper.aliasmap.put(subl.getRightItem().getAlias(), String.valueOf(Bootstrapper.addcount));
            }
            om.Projection.clear();
        }
        List<Column> groupCols = pl.getGroupByColumnReferences();
        Expression haveexp = pl.getHaving();
        Distinct dist = pl.getDistinct();
        List<OrderByElement> ob = pl.getOrderByElements();
        Limit lim = pl.getLimit();
        PopulateAliasesAndOutputFromList(join, ft, Bootstrapper.aliasmap, om); // aliases of each table
        Table fromTable;
        if (ft instanceof Table) {
            fromTable = (Table) ft;
        } else {
            fromTable = new Table();
            fromTable.setAlias(((SubSelect) ft).getAlias());
            fromTable.setName(Bootstrapper.aliasmap.get(((SubSelect) ft).getAlias()));
        }
        List<SelectItem> projs = pl.getSelectItems();
        populateProjection(projs, fromTable, om);
        ArrayList<jointuple> jts;
        //where clause
        Expression whereex = pl.getWhere();
        if (whereex != null) {
            int countofInsub = 0;
            if (Bootstrapper.subsel != null) {
                countofInsub = Bootstrapper.subsel.size();
            }
            populatesubselects(whereex, Bootstrapper.aliasmap, join, ft, om, joininfo);
            if (Bootstrapper.subsel != null && Bootstrapper.subsel.size() > countofInsub) {
                int num = om.Projection.size();
                for (SubSelect sl : Bootstrapper.subsel) {
                    ArrayList<jointuple> inlis = ExecuteSelect(sl.getSelectBody(), om);
                    Set<String> forin = new HashSet<String>();

                    for (jointuple inli : inlis) {
                        forin.add(inli.jointup.get(String.valueOf(Bootstrapper.addcount)));
                    }
                    Bootstrapper.InList.put(sl, forin);
                    inlis = null;
                }
                ArrayList<String> dellist = new ArrayList<>();
                for (int i = num; i < om.Projection.size(); i++) {
                    om.Selection.add(om.Projection.get(i));
                    dellist.add(om.Projection.get(i));
                }
                om.Projection.removeAll(dellist);
            }

            //fetch first table to memory
//            Fetch fetch = new Fetch();
//            ArrayList<jointuple> memoryTable = ExecuteFilter(Bootstrapper.aliasmap, Tname, whereex);
//            if (join != null) {
            jts = JoinTables(join, whereex, Bootstrapper.aliasmap, subresult, joininfo);
//            } else {
//                jts = memoryTable;
//            }
        } else {
            Fetch fetch = new Fetch();
            jts = fetch.GetArrayOfStringsFromFile(fromTable.getName());
        }
        ArrayList<jointuple> rettup = null;
        HashMap<String, ArrayList<jointuple>> groups;
        boolean orderbyremaining = false;
        if (groupCols != null) {
            ProcessGroupBy(jts, groupCols, haveexp, Bootstrapper.aliasmap, om);
            groups = processPreHaving(jts, Bootstrapper.aliasmap, groupCols.get(0)); // why groupCols.get(0) //  hashmap of array list

            if (haveexp != null) {
                jts = processHaving(groups, haveexp);
            }
            if (ob != null) {
                OrderByElement obe = ob.get(0);
                Column ordercol = (Column) obe.getExpression();
                String colname = ordercol.getColumnName();
                String tabnam = ordercol.getTable().getName();
                if (tabnam != null) {
                    tabnam = GetTableName(tabnam);

                    om.OrderBy.add(tabnam + "." + colname);

                    sortorder(jts, colname.toLowerCase(), tabnam, obe.isAsc());
                } else {
                    jts = getUnique(groups);
                    orderbyremaining = true;
                }

//            if(lim!=null)
            }
        } else {
            groups = null;
//            groups = new HashMap<String, ArrayList<jointuple>>();
//            groups.put("one", jts);
//            rettup = OutPutTargets(jts, projs, Bootstrapper.aliasmap, dist);
        }

        rettup = OutPutTargets(groups, groupCols, jts, projs, Bootstrapper.aliasmap, dist);
        if (orderbyremaining) {
            OrderByElement obe = ob.get(0);
            Column ordercol = (Column) obe.getExpression();
            String colname = ordercol.getColumnName();
            om.OrderBy.add(colname);
            sortorder(rettup, colname, obe.isAsc());
        }

        if (lim != null) {
            long rowcount = lim.getRowCount();
            long coun = 0;
            ArrayList<jointuple> retnew = new ArrayList<>();
            for (jointuple object : rettup) {
                if (coun == rowcount) {
                    break;
                }
                retnew.add(object);
                coun++;
            }
            rettup = retnew;
        }

        return rettup;
    }

    private static void PopulateAliasesAndOutputFromList(List<Join> join, FromItem ft, HashMap<String, String> aliasmap, OutputModel om) {

        if (ft != null) {
            if (ft instanceof Table) {
                String tempname;
                aliasmap.put(((Table) ft).getAlias(), ((Table) ft).getName().toLowerCase());
                if (((Table) ft).getAlias() != null) {
                    tempname = Bootstrapper.aliasmap.get(((Table) ft).getAlias());
                    if (!om.From.contains(tempname)) {
                        om.From.add(tempname);
                    }
                } else {
                    tempname = ((Table) ft).getName();
                    if (!om.From.contains(tempname)) {
                        om.From.add(tempname);
                    }
                }
            } else if (((SubSelect) ft).getAlias() != null) {
//                        String tempname = Bootstrapper.aliasmap.get(((SubSelect) join1.getRightItem()).getAlias());

                String tempname = ((SubSelect) ft).getAlias();
                if (!om.From.contains(tempname)) {
                    om.From.add(tempname);
                }
            }
        }
        if (join != null) {
            for (Join join1 : join) {
                //just checking simple join for now
                if (join1.getRightItem() instanceof Table) {
                    String tempname;
                    aliasmap.put(((Table) join1.getRightItem()).getAlias(), ((Table) join1.getRightItem()).getName().toLowerCase());
                    if (((Table) join1.getRightItem()).getAlias() != null) {
                        tempname = Bootstrapper.aliasmap.get(((Table) join1.getRightItem()).getAlias());
                        if (!om.From.contains(tempname)) {
                            om.From.add(tempname);
                        }
                    } else {
                        tempname = ((Table) join1.getRightItem()).getName();
                        if (!om.From.contains(tempname)) {
                            om.From.add(tempname);
                        }
                    }
                }
                if (join1.getRightItem() instanceof SubSelect) {
                    if (((SubSelect) join1.getRightItem()).getAlias() != null) {
//                        String tempname = Bootstrapper.aliasmap.get(((SubSelect) join1.getRightItem()).getAlias());

                        String tempname = ((SubSelect) join1.getRightItem()).getAlias();
                        if (!om.From.contains(tempname)) {
                            om.From.add(tempname);
                        }
                    }
                }
            }
        }
    }

    private static ArrayList<jointuple> JoinTables(List<Join> join, Expression whereex, HashMap<String, String> almap, HashMap<String, ArrayList<jointuple>> subresult, ArrayList<joincols> joininfo) {
        populatereversealias();
        ArrayList<String> js = new ArrayList<>();
        String sofarjoined = "";
        ArrayList<jointuple> memoryTable = new ArrayList<>();
        String tname = ((Table) join.get(0).getRightItem()).getName();
        String name = GetTableName(tname);
        js.add(name);
        joincols jc1 = null;
        for (joincols object : joininfo) {
            if (object.table.equals(name)) {
                jc1 = object;
                break;
            }
        }
        HashMap<String, byte[]> inmemtable = new HashMap<>();
        Fetch firstfch = new Fetch();
        ArrayList<jointuple> firstres = firstfch.populatehash(inmemtable, jc1, whereex, join);
        if (join.size() == 1) {
            return firstres;
        }
        sofarjoined = name;
        System.out.println("Table in memory : " + sofarjoined);

        ArrayList<jointuple> result = new ArrayList<>();
        ArrayList<jointuple> IntermediateTale = memoryTable;
        customeval cuseval = new customeval(almap);

        String joinnaam = jc1.otable;
        System.out.println("joining " + sofarjoined + " with " + joinnaam);
        sofarjoined += " " + joinnaam;
        js.add(joinnaam);

        if (Bootstrapper.addedschema.contains(joinnaam)) {
            int scolnum = Bootstrapper.schemas.get(joinnaam).schema.get(jc1.ocolumn.toLowerCase()).COLID;
            String salias = Bootstrapper.reversealiasmap.get(joinnaam);
            for (jointuple object :subresult.get(salias)) {
                String stup = object.jointup.get(joinnaam);
                String skey1 = stup.split("\\|")[scolnum];
                if (skey1.length() > 6) {
                    skey1 = skey1.substring(0, 6);
                }
                
                
                custombufferreader scbr = new custombufferreader(inmemtable.get(skey1));
                String stemp2;
                while ((stemp2 = scbr.readoneline()) != null) {
                    try {
                        jointuple stjt = new jointuple();
                        stjt.jointup.put(name, stemp2);
                        stjt.jointup.put(joinnaam, stup);
                        cuseval.settuple(stjt);
                        if (cuseval.eval(whereex).toBool()) {
                            result.add(stjt);
                        }
                    } catch (PrimitiveValue.InvalidPrimitive ex) {
                        Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (SQLException ex) {
                        Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                
                
            }
        } else {
            TableReader tr1 = new TableReader(Bootstrapper.path + joinnaam);
            String temp1;
            int count1 = 0;
            int colnum = Bootstrapper.schemas.get(joinnaam).schema.get(jc1.ocolumn.toLowerCase()).COLID;

            while ((temp1 = tr1.ReadTable()) != null) {
                if (count1 % 10000 == 0) {
                    System.out.println("processed " + count1 + " tuples from " + joinnaam + "...");
                }
                count1++;

                String key1 = temp1.split("\\|")[colnum];
                if (key1.length() > 6) {
                    key1 = key1.substring(0, 6);
                }

                custombufferreader cbr = new custombufferreader(inmemtable.get(key1));
                String temp2;
                while ((temp2 = cbr.readoneline()) != null) {
                    try {
                        jointuple tjt = new jointuple();
                        tjt.jointup.put(name, temp2);
                        tjt.jointup.put(joinnaam, temp1);
                        cuseval.settuple(tjt);
                        if (cuseval.eval(whereex).toBool()) {
                            result.add(tjt);
                        }
                    } catch (PrimitiveValue.InvalidPrimitive ex) {
                        Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (SQLException ex) {
                        Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

            }
        }
        IntermediateTale = result;
        inmemtable = null;

        System.gc();
        HashMap<String, ArrayList<jointuple>> interHash = new HashMap<>();
        while (true) {
            Bootstrapper.joincol = "";
            Bootstrapper.ojoincol = "";
            Bootstrapper.ojointab = "";
            Join join1 = null;

            if (js.size() == join.size()) {
                break;
            }
            join1 = SelectJoint(js, join, joininfo);

            populateinterhash(interHash, join1, IntermediateTale);

            System.out.println(
                    "Total number of tuples in memory : " + IntermediateTale.size());
            if (join1.getRightItem() instanceof SubSelect) {
                result = new ArrayList<>();

                String joinnamenot = ((SubSelect) join1.getRightItem()).getAlias();
                String AliasJoinNamenot = Bootstrapper.aliasmap.get(joinnamenot);
                System.out.println("joining " + sofarjoined + " with " + joinnamenot);
                sofarjoined += " " + joinnamenot;
                int count = 0;
                for (jointuple sub : subresult.get(joinnamenot)) {
                    if (count % 10000 == 0) {
                        System.out.println("processed " + count + " tuples from " + joinnamenot + "...");
                    }
                    count++;
                    ArrayList<jointuple> tempte;
                    if (!Bootstrapper.joincol.isEmpty()&&IntermediateTale.size()!=0&&IntermediateTale.get(0).jointup.containsKey(Bootstrapper.ojointab)) {
                        int colnum1 = Bootstrapper.schemas.get(AliasJoinNamenot).schema.get(Bootstrapper.joincol).COLID;
                        String key1 = sub.jointup.get(AliasJoinNamenot).split("\\|")[colnum1];
                        if (key1.length() > 4) {
                            key1 = key1.substring(0, 4);
                        }
                        tempte = interHash.get(key1);
                        if (tempte == null) {
                            continue;
                        }
                    } else {
                        tempte = IntermediateTale;

                    }
                    for (jointuple mem : tempte) {
                        try {
                            mem.jointup.put(AliasJoinNamenot, sub.jointup.get(AliasJoinNamenot));
                            cuseval.settuple(mem);
                            if (cuseval.eval(whereex).toBool()) {
                                jointuple jt = new jointuple();
                                for (Map.Entry<String, String> entry : mem.jointup.entrySet()) {
                                    String key = entry.getKey();
                                    String value = entry.getValue();
                                    jt.jointup.put(key, value);
                                }
                                result.add(jt);
                            }
                            mem.jointup.remove(AliasJoinNamenot);
                        } catch (PrimitiveValue.InvalidPrimitive ex) {
                            ex.printStackTrace();
                        } catch (SQLException ex) {
                            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
                IntermediateTale = result;

            } else {
                String joinname = ((Table) join1.getRightItem()).getName();
                joinname = GetTableName(joinname);
                result = new ArrayList<>();
                System.out.println("joining " + sofarjoined + " with " + joinname);
                sofarjoined += " " + joinname;

                TableReader tr = new TableReader(Bootstrapper.path + joinname);
                String temp;
                int count = 0;
                while ((temp = tr.ReadTable()) != null) {
                    if (count % 10000 == 0) {
                        System.out.println("processed " + count + " tuples from " + joinname + "...");
                    }
                    count++;
                    ArrayList<jointuple> tempte1;
                    if (!Bootstrapper.joincol.isEmpty()&&IntermediateTale.size()!=0&&IntermediateTale.get(0).jointup.containsKey(Bootstrapper.ojointab)) {
                        int colnum2 = Bootstrapper.schemas.get(joinname).schema.get(Bootstrapper.joincol).COLID;
                        String key2 = temp.split("\\|")[colnum2];
                        if (key2.length() > 4) {
                            key2 = key2.substring(0, 4);
                        }
                        tempte1 = interHash.get(key2);

                        if (tempte1 == null) {
                            continue;
                        }
                    } else {
                        tempte1 = IntermediateTale;
                    }
                    for (jointuple mem : tempte1) {
                        try {
                            mem.jointup.put(joinname, temp);
                            cuseval.settuple(mem);
                            if (cuseval.eval(whereex).toBool()) {
                                jointuple jt = new jointuple();
                                for (Map.Entry<String, String> entry : mem.jointup.entrySet()) {
                                    String key = entry.getKey();
                                    String value = entry.getValue();
                                    jt.jointup.put(key, value);
                                }
                                result.add(jt);
                            }
                            mem.jointup.remove(joinname);
                        } catch (PrimitiveValue.InvalidPrimitive ex) {
                            ex.printStackTrace();
                        } catch (SQLException ex) {
                            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }

                }
                IntermediateTale = result;
            }
        }
        return result;
    }

    private static void ProcessGroupBy(ArrayList<jointuple> jts, List<Column> groupCols, Expression haveexp, HashMap<String, String> aliasmap, OutputModel om) {

        String tableName = groupCols.get(0).getTable().getName(); // get name of table
        tableName = GetTableName(tableName);
        String col = groupCols.get(0).getColumnName(); // name of the column

        om.GroupBy.add(tableName + "." + col); // to print the output
        jts.sort(new CustomComp(tableName, col.toLowerCase())); // to convert to lower case
    }

    private static ArrayList<jointuple> ExecuteFilter(HashMap<String, String> aliasmap, String name, Expression whereex) {
        customeval cuseval = new customeval(aliasmap);
        ArrayList<jointuple> jts = new ArrayList<>();
        TableReader tr = new TableReader(Bootstrapper.path + name);
        String temp;
        name = GetTableName(name);
        if (whereex != null) {

            while ((temp = tr.ReadTable()) != null) {
                try {
                    jointuple jt = new jointuple();
                    jt.jointup.put(name, temp);

                    cuseval.settuple(jt);
                    PrimitiveValue pv = cuseval.eval(whereex);

                    if (pv.toBool()) {
                        jts.add(jt);
                    }

                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

            }
        } else {
            while ((temp = tr.ReadTable()) != null) {
                jointuple jt = new jointuple();
                jt.jointup.put(name, temp);

                jts.add(jt);

            }
        }
        return jts;

    }

    private static ArrayList<jointuple> ExecuteFilter(HashMap<String, String> aliasmap, ArrayList<jointuple> memoryTable, Expression whereex) {
        if (whereex != null) {
            customeval cuseval = new customeval(aliasmap);
            ArrayList<jointuple> jts = new ArrayList<>();
            for (jointuple jt : memoryTable) {
                try {
                    cuseval.settuple(jt);
                    PrimitiveValue pv = cuseval.eval(whereex);

                    if (pv.toBool()) {
                        jts.add(jt);
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

            }
            return jts;
        } else {
            return memoryTable;
        }
    }

    private static HashMap<String, ArrayList<jointuple>> processPreHaving(ArrayList<jointuple> jts, HashMap<String, String> aliasmap, Column groupCol) {
        String tableName = groupCol.getTable().getName();
        tableName = GetTableName(tableName);
        if (tableName == null) { // why this condition ??? 
            Object[] ob = jts.get(0).jointup.keySet().toArray();
            tableName = ob[0].toString();
        }
        String colname = groupCol.getColumnName();
        int col = Bootstrapper.schemas.get(tableName).schema.get(colname.toLowerCase()).COLID; // get the column id 
        HashMap<String, ArrayList<jointuple>> groups = new HashMap<>();
        String candidate = jts.get(0).jointup.get(tableName).split("\\|")[col];
        ArrayList<jointuple> group = new ArrayList<>(); 
        
        for (jointuple jt : jts) {
            if (!jt.jointup.get(tableName).split("\\|")[col].equals(candidate)) {

                groups.put(candidate, group);
                group = new ArrayList<>();
                candidate = jt.jointup.get(tableName).split("\\|")[col];

            }
            group.add(jt);
        }
        groups.put(candidate, group);
        jts.clear();
        for (Map.Entry<String, ArrayList<jointuple>> entry : groups.entrySet()) {
            jts.add(entry.getValue().get(0));
        }
        return groups;
    }

    private static void populatesubselects(Expression whereex, HashMap<String, String> aliasmap, List<Join> join, FromItem ft, OutputModel om, ArrayList<joincols> joininfo) {
        TestEval cu = new TestEval(aliasmap, om, joininfo);
        String largestFile = "";
        long len = 0;
        try {
            jointuple jt = new jointuple();
            if (join != null) {
                for (Join join1 : join) {
                    String name;
                    if (join1.getRightItem() instanceof SubSelect) {
                        name = ((SubSelect) join1.getRightItem()).getAlias();
                    } else {
                        name = ((Table) join1.getRightItem()).getName();
                        long temp = GetFileSize(name);
                        if (temp > len) {
                            largestFile = name;
                            len = temp;
                        }

                    }
                    name = GetTableName(name);
                    Schema sc = Bootstrapper.schemas.get(name);
                    String[] newtup = new String[sc.schema.size()];
                    int count = 0;
                    for (Map.Entry<String, colType> entry : sc.schema.entrySet()) {
                        if (entry.getValue().type.getDataType().equals("INTEGER")) {
                            newtup[entry.getValue().COLID] = "1";
                        } else if (entry.getValue().type.getDataType().equals("DATE")) {
                            newtup[entry.getValue().COLID] = "2017-01-01";
                        } else {
                            newtup[entry.getValue().COLID] = "1";
                        }
                        count++;
                    }
                    String newt = "";
                    for (String string : newtup) {
                        newt = newt + string + "|";
                    }
                    jt.jointup.put(name, newt);
                }
            }
            if (ft != null) {
                if (ft instanceof Table) {
                    String name = ((Table) ft).getName();
                    long temp = GetFileSize(name);
                    if (temp > len) {
                        largestFile = name;
                        len = temp;
                    }
                    name = GetTableName(name);
                    Schema sc = Bootstrapper.schemas.get(name);
                    String[] newtup = new String[sc.schema.size()];
                    int count = 0;
                    for (Map.Entry<String, colType> entry : sc.schema.entrySet()) {
                        if (entry.getValue().type.getDataType().equals("INTEGER")) {
                            newtup[entry.getValue().COLID] = "1";
                        } else if (entry.getValue().type.getDataType().equals("DATE")) {
                            newtup[entry.getValue().COLID] = "2017-01-01";
                        } else {
                            newtup[entry.getValue().COLID] = "1";
                        }
                        count++;
                    }
                    String newt = "";
                    for (String string : newtup) {
                        newt = newt + string + "|";
                    }
                    jt.jointup.put(name, newt);
                } else {
                    String name = ((SubSelect) ft).getAlias();

                    name = GetTableName(name);
                    Schema sc = Bootstrapper.schemas.get(name);
                    String[] newtup = new String[sc.schema.size()];
                    int count = 0;
                    for (Map.Entry<String, colType> entry : sc.schema.entrySet()) {
                        if (entry.getValue().type.getDataType().equals("INTEGER")) {
                            newtup[entry.getValue().COLID] = "1";
                        } else if (entry.getValue().type.getDataType().equals("DATE")) {
                            newtup[entry.getValue().COLID] = "2017-01-01";
                        } else {
                            newtup[entry.getValue().COLID] = "1";
                        }
                        count++;
                    }
                    String newt = "";
                    for (String string : newtup) {
                        newt = newt + string + "|";
                    }
                    jt.jointup.put(name, newt);
                }
            }

            cu.settuple(jt);
            cu.eval(whereex);
            ReorderJoin(join, largestFile, ft);

        } catch (SQLException ex) {
            Logger.getLogger(Database.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static boolean GetSubSelectPresent(List<Join> join) {
        if (join == null) {
            return false;
        }
        boolean ispresent = false;
        for (Join join1 : join) {
            if (join1.getRightItem() instanceof SubSelect) {
                if (Bootstrapper.fromsubsel == null) {
                    Bootstrapper.fromsubsel = new ArrayList<>();
                }
                Bootstrapper.fromsubsel.add(join1);
                ispresent = true;
            }
        }
        return ispresent;
    }

    private static String makenewSchema(List<SelectItem> projs, ArrayList<jointuple> jts, HashMap<String, String> aliasmap) {
//        ArrayList<jointuple> jts = null;
//        for (Map.Entry<String, ArrayList<jointuple>> entry : groups.entrySet()) {
//            jts = entry.getValue();
//            break;
//        }
        Schema sc = new Schema();
        int count = 0;
        for (SelectItem proj : projs) {
            if (proj instanceof AllColumns) {
                String tabnam = "";
                for (Map.Entry<String, String> entry : jts.get(0).jointup.entrySet()) {
                    tabnam = entry.getKey();
                    break;
                }
                for (Map.Entry<String, colType> entry : Bootstrapper.schemas.get(tabnam).schema.entrySet()) {
                    sc.schema.put(entry.getKey(), new colType(entry.getValue().COLID + count, entry.getValue().type, tabnam));
                }
                count += Bootstrapper.schemas.get(tabnam).schema.size();
            }
            if (proj instanceof AllTableColumns) {
                String name = ((AllTableColumns) proj).getTable().getName();
                name = GetTableName(name);
                for (Map.Entry<String, colType> entry : Bootstrapper.schemas.get(name).schema.entrySet()) {
                    sc.schema.put(entry.getKey(), new colType(entry.getValue().COLID + count, entry.getValue().type, name));
                }
                count += Bootstrapper.schemas.get(name).schema.size();

            }
            if (proj instanceof SelectExpressionItem) {
                if (((SelectExpressionItem) proj).getExpression() instanceof Column) {
                    String colname = ((Column) ((SelectExpressionItem) proj).getExpression()).getColumnName();
                    String name = ((Column) ((SelectExpressionItem) proj).getExpression()).getTable().getName();
                    name = GetTableName(name);
                    colType c = Bootstrapper.schemas.get(name).schema.get(colname.toLowerCase());
                    sc.schema.put(colname, new colType(count++, c.type, name));
                } else if (((SelectExpressionItem) proj).getExpression() instanceof Function) {
                    Function fn = (Function) ((SelectExpressionItem) proj).getExpression();
                    if (fn.getName().toUpperCase().equals("COUNT")) {
                        Expression exp = ((Function) ((SelectExpressionItem) proj).getExpression()).getParameters().getExpressions().get(0);
                        String colname = ((Column) exp).getColumnName();
                        String name = ((Column) exp).getTable().getName();
                        name = GetTableName(name);
                        colType c = Bootstrapper.schemas.get(name).schema.get(colname.toLowerCase());
                        sc.schema.put(((SelectExpressionItem) proj).getAlias(), new colType(count++, c.type, name));
                    }
                    if (fn.getName().toUpperCase().equals("SUM")) {
                        try {
                            Expression exp = ((Function) ((SelectExpressionItem) proj).getExpression()).getParameters().getExpressions().get(0);
                            ColDataType cd = new ColDataType();
                            GetDataTypeInExp gdt = new GetDataTypeInExp(Bootstrapper.aliasmap);
                            gdt.settuple(jts.get(0));
                            gdt.setcdt(cd);
                            gdt.eval(exp);
                            ColDataType finalgct = gdt.getcdt();
                            sc.schema.put(((SelectExpressionItem) proj).getAlias(), new colType(count++, finalgct));
                        } catch (SQLException ex) {
                            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            }

        }
        Bootstrapper.schemas.put(String.valueOf((++Bootstrapper.addcount)), sc);
        Bootstrapper.addedschema.add(String.valueOf(Bootstrapper.addcount));
        return String.valueOf(Bootstrapper.addcount);
    }

    private static void sortorder(ArrayList<jointuple> jts, String colname, boolean asc) {
        Object[] temp = jts.get(0).jointup.keySet().toArray();
        String tabnam = String.valueOf(temp[0]);
        jts.sort(new CustomComp(tabnam, colname, asc));
    }

    private static void sortorder(ArrayList<jointuple> jts, String colname, String tabnam, boolean asc) {
        jts.sort(new CustomComp(tabnam, colname, asc));
    }

    private static ArrayList<jointuple> processHaving(HashMap<String, ArrayList<jointuple>> groups, Expression haveexp) {
        ArrayList<String> evict = new ArrayList<>();
        ArrayList<jointuple> result = new ArrayList<>();
        for (Map.Entry<String, ArrayList<jointuple>> entry : groups.entrySet()) {
            try {
                customeval cuseval = new customeval(entry.getValue());
                if (!cuseval.eval(haveexp).toBool()) {
//                    groups.remove(entry.getKey());
                    evict.add(entry.getKey());
                } else {
                    result.add(entry.getValue().get(0));

//                    result.addAll(entry.getValue());
                }
            } catch (PrimitiveValue.InvalidPrimitive ex) {
                Logger.getLogger(Database.class
                        .getName()).log(Level.SEVERE, null, ex);

            } catch (SQLException ex) {
                Logger.getLogger(Database.class
                        .getName()).log(Level.SEVERE, null, ex);
            }

        }
        for (String string : evict) {
            groups.remove(string);
        }

        return result;
    }

    private static ArrayList<jointuple> OutPutTargets(HashMap<String, ArrayList<jointuple>> groups, List<Column> gcs, ArrayList<jointuple> joints, List<SelectItem> projs, HashMap<String, String> aliasmap, Distinct dist) {
        customeval cuseval = new customeval(aliasmap);
        ArrayList<jointuple> output = new ArrayList<>();
        String sc = makenewSchema(projs, joints, aliasmap);
        int width = Bootstrapper.schemas.get(sc).schema.size();
        Column c = null;
        if (gcs != null) {
            c = gcs.get(0);

        }

//        for (Map.Entry<String, ArrayList<jointuple>> entry : groups.entrySet()) {
//            String key = entry.getKey();
//            ArrayList<jointuple> jts = entry.getValue();
        for (jointuple jt : joints) {
//            String out = "";
            int count = 0;
            String[] tup = new String[width];
            jointuple newjt = new jointuple();
            for (SelectItem proj : projs) {
                if (proj instanceof AllColumns) {
                    for (Map.Entry<String, String> entry1 : jt.jointup.entrySet()) {
                        tup = entry1.getValue().split("\\|");
                    }
                }
                if (proj instanceof AllTableColumns) {
                    String tablename = ((AllTableColumns) proj).getTable().getName();
                    tablename = GetTableName(tablename);
                    String[] temptup = jt.jointup.get(tablename).split("\\|");
                    for (String atr : temptup) {
                        tup[count++] = atr;
                    }
                }
                if (proj instanceof SelectExpressionItem) {
                    try {
                        SelectExpressionItem se = (SelectExpressionItem) proj;
                        Expression ex = se.getExpression();
                        if (ex instanceof Function) {
                            String colnam = c.getColumnName();

                            String tabnam = c.getTable().getName();
                            tabnam = GetTableName(tabnam);
                            Schema shc = Bootstrapper.schemas.get(tabnam);
                            colType col = shc.schema.get(colnam.toLowerCase());
                            String temp = jt.jointup.get(tabnam).split("\\|")[col.COLID];
                            customeval cuseval1 = new customeval(groups.get(temp));
                            PrimitiveValue outval = cuseval1.eval(ex);
                            tup[count++] = outval.toRawString();
                        } else {
                            cuseval.settuple(jt);
                            PrimitiveValue outval = cuseval.eval(ex);
//                        out = out + outval + ",";
                            tup[count++] = outval.toRawString();
                        }
                    } catch (SQLException ex1) {
                        ex1.printStackTrace();
                    }
                }

            }
//            out = out.substring(0, out.length() - 1);
//            output.add(out);
////            System.out.println(out);
//            jt = null;
            String newt = "";
            for (String string : tup) {
                newt = newt + string + "|";
            }
            newt = newt.substring(0, newt.length() - 1);
            newjt.jointup.put(sc, newt);
            output.add(newjt);
        }
//        }

        if (dist != null) {
            HashMap<String, jointuple> finallst = new HashMap<>();
            for (jointuple jt : output) {
                String key = jt.jointup.get(sc);
//                for (String ob : jt.jointup.get(sc)) {
//                    key += ob;
//                }
//                String key = jt.jointup.get(sc)[0];
                if (!finallst.containsKey(key)) {
                    finallst.put(key, jt);
                }
            }
            return new ArrayList<>(finallst.values());
        }
        return output;
    }

    private static ArrayList<jointuple> getUnique(HashMap<String, ArrayList<jointuple>> groups) {
        ArrayList<jointuple> jts = new ArrayList<>();
        for (Map.Entry<String, ArrayList<jointuple>> entry : groups.entrySet()) {
            jts.add(entry.getValue().get(0));
        }
        return jts;
    }

    private static void populateProjection(List<SelectItem> sls, Table ft, OutputModel om) {
        for (SelectItem sl : sls) {
            if (sl instanceof SelectExpressionItem) {
                if (((SelectExpressionItem) sl).getExpression() instanceof Column) {
                    String out = "";
                    SelectExpressionItem selex = (SelectExpressionItem) sl;
                    Column c = (Column) selex.getExpression();
                    String tab = c.getTable().getName();
                    tab = GetTableName(tab);
                    out += tab + ".";
                    out += c.getColumnName();
                    if (!om.Projection.contains(out)) {
                        om.Projection.add(out);
                    }
                }
                if (((SelectExpressionItem) sl).getExpression() instanceof Function) {
                    String out = "";
                    SelectExpressionItem selex = (SelectExpressionItem) sl;
                    Function ex = (Function) selex.getExpression();
                    String fn = ex.getName();
                    ExpressionList el = ex.getParameters();
                    List<Expression> els = el.getExpressions();
                    if (fn.toUpperCase().equals("COUNT")) {

                        out = ex.toString() + "as ";

                        out += selex.getAlias();
                        if (!om.Projection.contains(out)) {
                            om.Projection.add(out);
                        }
                    } else if (fn.toUpperCase().equals("SUM")) {
                        out = ex.toString() + " as ";
                        out += selex.getAlias();
                        if (!om.Projection.contains(out)) {
                            om.Projection.add(out);
                        }
                    }
                }
            }
            if (sl instanceof AllColumns) {
                String tabname = ft.getName();
                String tabalias = tabname;
                tabname = GetTableName(tabname);
                ArrayList<Map.Entry<String, colType>> temp = new ArrayList<>();
                for (Map.Entry<String, colType> entry : Bootstrapper.schemas.get(tabname).schema.entrySet()) {

                    temp.add(entry);

                }
                temp.sort(new EntryComp());
                for (Map.Entry<String, colType> entry : temp) {
                    String out = "";
                    out += tabname + "." + entry.getKey();
                    if (!om.Projection.contains(out)) {
                        om.Projection.add(out);
                    }
                }
            }
            if (sl instanceof AllTableColumns) {
                AllTableColumns atc = (AllTableColumns) sl;
                String tab = atc.getTable().getName();
                String tabalias = tab;
                tab = GetTableName(tab);
                Schema sc = Bootstrapper.schemas.get(tab);
                ArrayList<Map.Entry<String, colType>> temp = new ArrayList<>();

                for (Map.Entry<String, colType> entry : sc.schema.entrySet()) {
                    temp.add(entry);

                }
                temp.sort(new EntryComp());

                for (Map.Entry<String, colType> entry : temp) {
                    String key = entry.getKey();
                    String out = "";
                    out += tab + "." + key;
                    if (!om.Projection.contains(out)) {
                        om.Projection.add(out);
                    }
                }
            }
        }
    }

    private static void DisplaySummary(OutputModel om) {
        String out = "PROJECTION" + " : ";
        for (String s : om.Projection) {
            out += s + ", ";
        }
        out = out.substring(0, out.length() - 2);
        consoleop(out);
        out = "FROM" + " : ";
        for (String s : om.From) {
            out += s + ", ";
        }
        out = out.substring(0, out.length() - 2);
        consoleop(out);
        out = "SELECTION" + " : ";
        for (String s : om.Selection) {
            out += s + ", ";
        }
        if (om.Selection.size() == 0) {
            out += "NULL";
        } else {
            out = out.substring(0, out.length() - 2);
        }
        consoleop(out);

        out = "JOIN" + " : ";
        for (String s : om.Join) {
            out += s + ", ";
        }
        if (om.Join.size() == 0) {
            out += "NULL";
        } else {
            out = out.substring(0, out.length() - 2);
        }
        consoleop(out);

        out = "GROUPBY" + " : ";
        for (String s : om.GroupBy) {
            out += s + ", ";
        }
        if (om.GroupBy.size() == 0) {
            out += "NULL";
        } else {
            out = out.substring(0, out.length() - 2);
        }
        consoleop(out);

        out = "ORDERBY" + " : ";
        for (String s : om.OrderBy) {
            out += s + ", ";
        }
        if (om.OrderBy.size() == 0) {
            out += "NULL";
        } else {
            out = out.substring(0, out.length() - 2);
        }
        consoleop(out);
        consoleop("");
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

    private static long GetFileSize(String name) {
        String path = Bootstrapper.path + name + ".csv";
        File fl = new File(path);
        return fl.length();
    }

    private static void ReorderJoin(List<Join> join, String largestFile, FromItem ft) {

        Join newj = new Join();
        newj.setRightItem(ft);
        join.add(newj);
        join.sort(new joinsorter(largestFile));
    }

    private static Join SelectJoint(ArrayList<String> js, List<Join> join, ArrayList<joincols> joininfo) {
        Join join1 = null;
        for (Join jo : join) {
            if (jo.getRightItem() instanceof Table) {
                String joname = ((Table) jo.getRightItem()).getName();
                joname = GetTableName(joname);
                if (js.contains(joname)) {
                    continue;
                } else {
                    for (joincols object : joininfo) {
                        if (object.table.equals(joname)) {
                            if (js.contains(object.otable)) {
                                js.add(joname);
                                Bootstrapper.joincol = object.column;
                                Bootstrapper.ojoincol = object.ocolumn;
                                Bootstrapper.ojointab = object.otable;
                                return jo;
                            }
                        }
                    }
                }
            } else {
                String joname = ((SubSelect) jo.getRightItem()).getAlias();
                joname = GetTableName(joname);
                if (js.contains(joname)) {
                    continue;
                } else {
                    for (joincols object : joininfo) {
                        if (object.table.equals(joname)) {
                            if (js.contains(object.otable)) {
                                js.add(joname);
                                Bootstrapper.joincol = object.column;
                                Bootstrapper.ojoincol = object.ocolumn;
                                Bootstrapper.ojointab = object.otable;
                                return jo;
                            }
                        }
                    }
                }
            }
        }
        if (join1 == null) {
            for (Join jo : join) {
                if (jo.getRightItem() instanceof Table) {
                    String joname = ((Table) jo.getRightItem()).getName();
                    joname = GetTableName(joname);
                    if (js.contains(joname)) {
                        continue;
                    } else {

                        for (joincols object : joininfo) {
                            if (object.table.equals(joname)) {
                                Bootstrapper.joincol = object.column;
                                Bootstrapper.ojoincol = object.ocolumn;
                                Bootstrapper.ojointab = object.otable;
                            }
                        }
                        js.add(joname);
                        return jo;
                    }
                } else {
                    String joname = ((SubSelect) jo.getRightItem()).getAlias();
                    joname = GetTableName(joname);
                    if (js.contains(joname)) {
                        continue;
                    } else {
                        for (joincols object : joininfo) {
                            if (object.table.equals(joname)) {
                                if (js.contains(object.otable)) {
                                    js.add(joname);
                                    Bootstrapper.joincol = object.column;
                                    Bootstrapper.ojoincol = object.ocolumn;
                                    Bootstrapper.ojointab = object.otable;
                                    return jo;
                                }
                            }
                        }
                    }
                }
            }
        }
        return join1;
    }

    private static void populateinterhash(HashMap<String, ArrayList<jointuple>> interHash, Join join1, ArrayList<jointuple> IntermediateTale) {
        if (Bootstrapper.ojointab.isEmpty()) {
            return;
        }
        if(IntermediateTale.size()==0)
            return;
        if(!IntermediateTale.get(0).jointup.containsKey(Bootstrapper.ojointab))
            return;
        interHash.clear();
        int col = Bootstrapper.schemas.get(Bootstrapper.ojointab).schema.get(Bootstrapper.ojoincol).COLID;
        for (jointuple object : IntermediateTale) {

            String key = object.jointup.get(Bootstrapper.ojointab).split("\\|")[col];
            if (key.length() > 4) {
                key = key.substring(0, 4);
            }
            if (interHash.containsKey(key)) {
                interHash.get(key).add(object);
            } else {
                ArrayList<jointuple> jts = new ArrayList<>();
                jts.add(object);
                interHash.put(key, jts);

            }
        }
    }

    private static void populatereversealias() {
        for (Map.Entry<String, String> entry : Bootstrapper.aliasmap.entrySet()) {
            Bootstrapper.reversealiasmap.put(entry.getValue(), entry.getKey());
        }
    }

    public static class EntryComp implements Comparator<Map.Entry<String, colType>> {

        @Override
        public int compare(Map.Entry<String, colType> o1, Map.Entry<String, colType> o2) {
            return o1.getValue().COLID - o2.getValue().COLID;
        }

    }

    public static class CustomComp implements Comparator<jointuple> {

        String Tname;
        String Cname;
        boolean Asc;

        private CustomComp(String tableName, String col) {
            Tname = tableName;
            Cname = col;
            Asc = true;
        }

        private CustomComp(String tableName, String col, boolean asc) {
            Tname = tableName;
            Cname = col;
            Asc = asc;
        }

        @Override
        public int compare(jointuple o1, jointuple o2) {
            colType col = Bootstrapper.schemas.get(Tname).schema.get(Cname);
            int colid = col.COLID;
            ColDataType typr = col.type;
            if (!o1.jointup.containsKey(Tname)) {
                System.out.println("");
            }
            String a1 = o1.jointup.get(Tname).split("\\|")[colid];
            String a2 = o2.jointup.get(Tname).split("\\|")[colid];
            if (typr.getDataType().equals("INTEGER")) {

                if (Asc) {
                    return Integer.parseInt(a1) - Integer.parseInt(a2);
                } else {
                    return Integer.parseInt(a2) - Integer.parseInt(a1);
                }
            } else if (typr.getDataType().equals("DOUBLE")) {

                if (Asc) {
                    return (Double.parseDouble(a1) - Double.parseDouble(a2)) > 0 ? 1 : -1;
                } else {
                    return (Double.parseDouble(a2) - Double.parseDouble(a1)) > 0 ? 1 : -1;
                }
            } else if (typr.getDataType().equals("LONG")) {
                if (Asc) {
                    return (Long.parseLong(a1) - Long.parseLong(a2)) > 0 ? 1 : -1;
                } else {
                    return (Long.parseLong(a2) - Long.parseLong(a1)) > 0 ? 1 : -1;
                }
            } else if (typr.getDataType().equals("DATE")) {
                try {
                    Date date1 = new SimpleDateFormat("YYYY-MM-DD").parse(a1);
                    Date date2 = new SimpleDateFormat("YYYY-MM-DD").parse(a2);
                    if (Asc) {
                        return date1.compareTo(date2);
                    } else {
                        return date2.compareTo(date1);
                    }
                } catch (java.text.ParseException ex) {
                    return 0;
                }
            }
            if (Asc) {
                return o1.jointup.get(Tname).split("\\|")[colid].compareTo(o2.jointup.get(Tname).split("\\|")[colid]);
            } else {
                return o2.jointup.get(Tname).split("\\|")[colid].compareTo(o1.jointup.get(Tname).split("\\|")[colid]);
            }

        }

    }
}
