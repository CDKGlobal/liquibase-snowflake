package com.hp.db;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 03/11/13
 * Time: 13:31
 * To change this template use File | Settings | File Templates.
 */

import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
public class ConnectionManager {
    public void stupid(){
        try{
            throw new LiquibaseException("dont know why");
        }catch (Exception e){

        }
    }
    public void stupid2(){
        try{
            throw new DatabaseException("dont know why");
        }catch (Exception e){

        }
    }
}
