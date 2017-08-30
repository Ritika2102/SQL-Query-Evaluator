/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Rakshit
 */
public class custombufferreader {

    byte[] buffr;
    int current = 0;
    char del = '~';
    
    public custombufferreader(byte[] buff) {
        buffr = buff;
//        try {
//            del = "~".getBytes("UTF-8")[0];
//        } catch (UnsupportedEncodingException ex) {
//            Logger.getLogger(custombufferreader.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    
    
    
     public  String readoneline() {
        StringBuilder buildS = new StringBuilder();
        char item;
        if(buffr==null)
            return null;
        if(current==buffr.length)
            return null;
        for (int j = current; j < buffr.length; j++) {
            current++;
            if ((item =  (char)buffr[j]) != del) {
                buildS = buildS.append(item);
            } 
            else
                break;
        }
        return buildS.toString();
    }
}
