/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.util.*;
import java.sql.SQLException;
import java.util.HashMap;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 *
 * @author Rakshit
 */
public class customeval extends Eval {

    String tablename; 
    jointuple tuple; // to be evaluated 
    Schema sc; // we get the schema
    HashMap<String, String> aliasmap; 
    ArrayList<jointuple> Group; // pass the entire group to lib

    public customeval(ArrayList<jointuple> group) {
        super();
        Group = group;
    }

    public customeval(HashMap<String, String> almap) {
        super();
        aliasmap = almap;

    }

    jointuple gettuple() {
        return tuple;
    }

    void settuple(jointuple tup) {
        tuple = tup;
    }

    @Override
    public PrimitiveValue eval(InExpression in) throws SQLException {
        //        for (jointuple jt : Bootstrapper.InList.get(((SubSelect) in.getItemsList()))) {
        //            String[] tup = jt.jointup.get(temp[0]).split("\\|");
        //            if (tup[0].equals(val.toRawString())) {
        //                return BooleanValue.TRUE;
        //            }
        //        }
        if (in.getItemsList() instanceof SubSelect) { // to diffrentiate between the two in we have
            PrimitiveValue val = eval(in.getLeftExpression());
            if (Bootstrapper.InList.get(((SubSelect) in.getItemsList())).contains(val.toRawString())) {
                return BooleanValue.TRUE;
            } else {
                return BooleanValue.FALSE;
            }
        } else {
            ExpressionList exps = (ExpressionList) in.getItemsList();
            for (Expression object : exps.getExpressions()) {
                PrimitiveValue val = eval(in.getLeftExpression());
                if (val instanceof NotPresent) {
                    return BooleanValue.TRUE;
                }
                PrimitiveValue x = eval(object);
                if (val.equals(x)) {
                    return BooleanValue.TRUE;
                }
            }
            return BooleanValue.FALSE;
        }
    }

    @Override

    public PrimitiveValue eval(LikeExpression like) throws SQLException {
        if (eval(like.getLeftExpression()) instanceof NotPresent) {
            return BooleanValue.TRUE;
        }
        return super.eval(like);
    }

    @Override
    public PrimitiveValue eval(EqualsTo a)
            throws SQLException {
        PrimitiveValue lhs = eval(a.getLeftExpression());
        PrimitiveValue rhs = eval(a.getRightExpression());

        if (lhs == null || rhs == null) {
            return BooleanValue.FALSE;
        }
        if (lhs instanceof NotPresent || rhs instanceof NotPresent) {
            return BooleanValue.TRUE;
        }

        return lhs.equals(rhs) ? BooleanValue.TRUE : BooleanValue.FALSE;
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

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        if (tuple == null) {
            return new NotPresent();
        }
        tablename = column.getTable().getName();
        tablename = GetTableName(tablename);
        if (!tuple.jointup.containsKey(tablename)) {
            return new NotPresent();
        }
        sc = Bootstrapper.schemas.get(tablename);  // get schema
        colType col = sc.schema.get(column.getColumnName().toLowerCase()); // get columnID
 
        String s;
        s = tuple.jointup.get(tablename).split("\\|")[col.COLID]; // then i will get the value from tuple
 
        if (col.type.getDataType().equals("INTEGER")) { // check its type
            if (s.equals("null") || s.equals("NULL")) {
                return new NullValue();
            }
            return new LongValue(s);
        } else if (col.type.getDataType().equals("DOUBLE")) {
            if (s.equals("null") || s.equals("NULL")) {
                return new NullValue();
            }
            return new DoubleValue(s);
        } else if (col.type.getDataType().equals("DATE")) {
            if (s.equals("null") || s.equals("NULL")) {
                return new NullValue();
            }
            return new DateValue(s); // but return datevalue because it is a string
        }
        if (s.equals("null") || s.equals("NULL")) {
            return new NullValue();
        }
        return new StringValue(s);
    }

    @Override
    public PrimitiveType escalateNumeric(PrimitiveType lhs, PrimitiveType rhs) //compare the values and return it
            throws SQLException {

        if (lhs == PrimitiveType.DATE || rhs == PrimitiveType.DATE) {
            return PrimitiveType.DATE;
        }
        if ((assertNumeric(lhs) == PrimitiveType.DOUBLE)
                || (assertNumeric(rhs) == PrimitiveType.DOUBLE)) {
            return PrimitiveType.DOUBLE;
        } else {
            return PrimitiveType.LONG;
        }
    }

    @Override
    public PrimitiveValue eval(Function function)
            throws SQLException {
        String fn = function.getName().toUpperCase();
        if ("DATE".equals(fn)) {
            List args = function.getParameters().getExpressions();
            if (args.size() != 1) {
                throw new SQLException("DATE() takes exactly one argument");
            }
            return new DateValue(eval((Expression) args.get(0)).toRawString());
        } else if ("COUNT".equals(fn.toUpperCase())) {
            return new LongValue(Group.size());
        } else if ("SUM".equals(fn.toUpperCase())) {
            Expression args = function.getParameters().getExpressions().get(0);
            double SUM = 0;
            for (jointuple object : Group) {
                tuple = object;
                PrimitiveValue temp = eval(args);
                SUM += temp.toDouble();

            }
            return new DoubleValue(SUM);
        }
        return missing("Function:" + fn);

    }

    @Override
    public PrimitiveValue cmp(BinaryExpression e, CmpOp op)
            throws SQLException {
        try {
            PrimitiveValue lhs = eval(e.getLeftExpression());
            PrimitiveValue rhs = eval(e.getRightExpression());
            if (lhs == null || rhs == null) {
                return null;
            }
            if (lhs instanceof NotPresent || rhs instanceof NotPresent) {
                return BooleanValue.TRUE;
            }
            boolean ret;

            switch (escalateNumeric(getPrimitiveType(lhs), getPrimitiveType(rhs))) {
                case DOUBLE:
                    ret = op.op(lhs.toDouble(), rhs.toDouble());
                    break;
                case LONG:
                    ret = op.op(lhs.toLong(), rhs.toLong());
                    break;
                case DATE: {

                    DateValue dlhs = new DateValue(lhs.toRawString()),
                            drhs = new DateValue(rhs.toRawString());
                    ret = op.op(
                            dlhs.getYear() * 10000
                            + dlhs.getMonth() * 100
                            + dlhs.getDate(),
                            drhs.getYear() * 10000
                            + drhs.getMonth() * 100
                            + drhs.getDate()
                    );
                }
                break;
                default:
                    throw new SQLException("Invalid PrimitiveType escalation");
            }
            return ret ? BooleanValue.TRUE : BooleanValue.FALSE;
        } catch (PrimitiveValue.InvalidPrimitive ex) {
            throw new SQLException("Invalid leaf value", ex);
        }
    }

    void settuple(jointuple jt, OutputModel om) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public class NotPresent implements PrimitiveValue {

        @Override
        public long toLong() throws InvalidPrimitive {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public double toDouble() throws InvalidPrimitive {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean toBool() throws InvalidPrimitive {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String toRawString() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public PrimitiveType getType() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void accept(ExpressionVisitor ev) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}
