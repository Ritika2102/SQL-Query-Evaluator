/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.sql.SQLException;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 *
 * @author Rakshit
 */
public class TestEval extends customeval {

    OutputModel OM;
    ArrayList<joincols> ji;

    public TestEval(HashMap<String, String> aliasmap, OutputModel om, ArrayList<joincols> joininfo) {
        super(aliasmap);
        OM = om;
        ji = joininfo;
    }

    public TestEval(HashMap<String, String> aliasmap, OutputModel om) {
        super(aliasmap);
        OM = om;
    }

    jointuple gettuple() {
        return tuple;
    }

    void settuple(jointuple tup) {
        tuple = tup;
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
//    private static String GetTableName(String tabnam) {
//        if (tabnam == null) {
//            return Bootstrapper.aliasmap.get(tabnam);
//        }
//        if (!Bootstrapper.schemas.containsKey(tabnam.toLowerCase())) {
//            String res = Bootstrapper.aliasmap.get(tabnam);
//            if (Bootstrapper.addedschema.contains(res)) {
//
//                return tabnam;
//            }
//            return res;
//
//        } else {
//            return tabnam.toLowerCase();
//        }
//    }

    @Override
    public PrimitiveValue eval(NotEqualsTo a) throws SQLException {
        if (a.getRightExpression() instanceof StringValue || a.getRightExpression() instanceof LongValue || a.getRightExpression() instanceof BooleanValue) {
            Column c = (Column) a.getLeftExpression();
            String tabname = c.getTable().getName();
            tabname = GetTableName(tabname);
            String out = "";
            if (tabname != null) {
                out += tabname + ".";
            }
            out += c.getColumnName();
            OM.Selection.add(out);
        }
        return super.eval(a);
    }

    @Override
    public PrimitiveValue eval(GreaterThanEquals a) throws SQLException {
        if (a.getRightExpression() instanceof StringValue || a.getRightExpression() instanceof LongValue || a.getRightExpression() instanceof BooleanValue) {
            Column c = (Column) a.getLeftExpression();
            String tabname = c.getTable().getName();
            tabname = GetTableName(tabname);

            String out = "";
            if (tabname != null) {
                out += tabname + ".";
            }
            out += c.getColumnName();
            OM.Selection.add(out);
        }
        return super.eval(a);
    }

    @Override
    public PrimitiveValue eval(GreaterThan a) throws SQLException {
        if (a.getRightExpression() instanceof StringValue || a.getRightExpression() instanceof LongValue || a.getRightExpression() instanceof BooleanValue) {
            Column c = (Column) a.getLeftExpression();
            String tabname = c.getTable().getName();
            tabname = GetTableName(tabname);

            String out = "";
            if (tabname != null) {
                out += tabname + ".";
            }
            out += c.getColumnName();
            OM.Selection.add(out);
        }
        return super.eval(a);
    }

    @Override
    public PrimitiveValue eval(LikeExpression a) throws SQLException {
        if (a.getRightExpression() instanceof StringValue || a.getRightExpression() instanceof LongValue || a.getRightExpression() instanceof BooleanValue) {
            Column c = (Column) a.getLeftExpression();
            String tabname = c.getTable().getName();
            tabname = GetTableName(tabname);

            String out = "";
            if (tabname != null) {
                out += tabname + ".";
            }
            out += c.getColumnName();
            OM.Selection.add(out);
        }
        return super.eval(a);
    }

    @Override
    public PrimitiveValue eval(EqualsTo a)
            throws SQLException {

        if ((a.getLeftExpression() instanceof Column) && (a.getRightExpression() instanceof Column)) {
            Column c = (Column) a.getLeftExpression();
            String tabname = c.getTable().getName();
            tabname = GetTableName(tabname);

            if (Bootstrapper.schemas.containsKey(tabname)) {
                String out = "";
                if (tabname != null && Bootstrapper.addedschema.contains(tabname)) {

                    String val = "";
                    for (Map.Entry<String, colType> entry : Bootstrapper.schemas.get(tabname).schema.entrySet()) {
                        if (entry.getKey().equals(c.getColumnName().toLowerCase())) {
                            val = entry.getValue().tabname;
                            break;
                        }

                    }
                    out += val + ".";

                } else if (tabname != null) {
                    out += tabname + ".";
                }
                out += c.getColumnName();
                OM.Join.add(out);
            } else {
                String res = Bootstrapper.aliasmap.get(tabname);
                String out = "";

                for (Map.Entry<String, colType> entry : Bootstrapper.schemas.get(res).schema.entrySet()) {
                    if (entry.getKey().equals(c.getColumnName().toLowerCase())) {
                        tabname = entry.getValue().tabname;
                        break;
                    }

                }

                if (tabname != null) {
                    out += tabname + ".";
                }
                out += c.getColumnName();
                OM.Join.add(out);
            }

            Column c1 = (Column) a.getRightExpression();
            String tabname1 = c1.getTable().getName();
            tabname1 = GetTableName(tabname1);
            if (Bootstrapper.schemas.containsKey(tabname1)) {
                String out1 = "";
                if (tabname1 != null && Bootstrapper.addedschema.contains(tabname1)) {
                    String val1 = "";
                    for (Map.Entry<String, colType> entry : Bootstrapper.schemas.get(tabname1).schema.entrySet()) {
                        if (entry.getKey().equals(c1.getColumnName().toLowerCase())) {
                            val1 = entry.getValue().tabname;
                            break;
                        }

                    }
                    out1 += val1 + ".";
                } else if (tabname1 != null) {
                    out1 += tabname1 + ".";
                }
                out1 += c1.getColumnName();
                OM.Join.add(out1);

            } else {
                String res1 = Bootstrapper.aliasmap.get(tabname1);
                String out = "";

                for (Map.Entry<String, colType> entry : Bootstrapper.schemas.get(res1).schema.entrySet()) {
                    if (entry.getKey().equals(c1.getColumnName().toLowerCase())) {
                        tabname1 = entry.getValue().tabname;
                        break;
                    }

                }

                if (tabname1 != null) {
                    out += tabname1 + ".";
                }
                out += c1.getColumnName();
                OM.Join.add(out);
            }
            ji.add(new joincols(tabname, c.getColumnName().toLowerCase(), tabname1, c1.getColumnName().toLowerCase()));
            ji.add(new joincols(tabname1, c1.getColumnName().toLowerCase(), tabname, c.getColumnName().toLowerCase()));

        }
        if (a.getRightExpression() instanceof StringValue || a.getRightExpression() instanceof LongValue || a.getRightExpression() instanceof BooleanValue) {
            Column c = (Column) a.getLeftExpression();
            String tabname = c.getTable().getName();
            tabname = GetTableName(tabname);

            String out = "";
            if (tabname != null) {
                out += tabname + ".";
            }
            out += c.getColumnName();
            OM.Selection.add(out);
        }

        return super.eval(a);
    }

    @Override
    public PrimitiveValue eval(InExpression in) throws SQLException {
        if (in.getItemsList() instanceof SubSelect) {
            if (Bootstrapper.subsel == null) {
                Bootstrapper.subsel = new ArrayList<>();
            }
            Bootstrapper.subsel.add((SubSelect) in.getItemsList());
        }
        Column c = (Column) in.getLeftExpression();
        String tabname = c.getTable().getName();
        tabname = GetTableName(tabname);

        String out = "";
        if (tabname != null) {
            out += tabname + ".";
        }
        out += c.getColumnName();
        OM.Selection.add(out);
        return BooleanValue.TRUE;
    }
}
