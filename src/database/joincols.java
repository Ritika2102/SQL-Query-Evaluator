/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

/**
 *
 * @author Rakshit
 */
public class joincols {

    public String table;
    public String column;
    public String otable;
    public String ocolumn;

    public joincols(String tab, String col, String otab, String ocol) {
        table = tab;
        column = col;
        otable = otab;
        ocolumn = ocol;
    }

}
