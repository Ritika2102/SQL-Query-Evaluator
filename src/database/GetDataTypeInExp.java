/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.sql.SQLException;
import java.util.HashMap;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.create.table.ColDataType;

/**
 *
 * @author Rakshit
 */
public class GetDataTypeInExp extends customeval {

    public ColDataType CDT;

    public void setcdt(ColDataType cdt) {
        CDT = cdt;
    }

    public ColDataType getcdt() {
        return CDT;
    }

    public GetDataTypeInExp(HashMap<String, String> almap) {
        super(almap);
        aliasmap = almap;

    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        if (CDT.getDataType() == null) {
            String tablename = column.getTable().getName();
            tablename = GetTableName(tablename);
            Schema sc = Bootstrapper.schemas.get(tablename);
            colType col = sc.schema.get(column.getColumnName().toLowerCase());
            CDT = col.type;
        }
        return super.eval(column);
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

}
