/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.util.*;


public class OutputModel {

    public ArrayList<String> Projection;
    public ArrayList<String> From;
    public ArrayList<String> Selection;
    public ArrayList<String> Join;
    public ArrayList<String> GroupBy;
    public ArrayList<String> OrderBy;

    public OutputModel() {
        Projection = new ArrayList<>();
        From = new ArrayList<>();
        Selection = new ArrayList<>();
        Join = new ArrayList<>();
        GroupBy = new ArrayList<>();
        OrderBy = new ArrayList<>();
    }

}
