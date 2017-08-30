/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.util.Comparator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 *
 * @author Rakshit
 */
public class joinsorter implements Comparator<Join> {

    String lar;
    public joinsorter(String largestFile) {
        lar = largestFile;
    }

    @Override
    public int compare(Join o1, Join o2) {
        if(o1.getRightItem() instanceof SubSelect||o2.getRightItem() instanceof SubSelect)
            return 1;
        if(((Table) o1.getRightItem()).getName().equals(((Table) o2.getRightItem()).getName()))
            return 0;
        if(((Table) o2.getRightItem()).getName().equals(lar))
            return 1;
        if(((Table) o1.getRightItem()).getName().equals(lar))
            return -1;
        else
            return 0;
    }
    
}
