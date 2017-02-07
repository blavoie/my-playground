package com.brunol.pdc.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class Application {

    public static void main(String[] args) {

        Cluster cluster;
        Session session;
        ResultSet results;
        Row rows;
       
        //
        //  http://docs.datastax.com/en/developer/java-driver/3.1/manual/load_balancing/
        // 
        //  If you don’t explicitly configure the policy, you get the default, which is a datacenter-aware, token-aware policy:
        //      new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build());
        //        
        // http://docs.datastax.com/en/developer/java-driver/3.1/manual/retries/
        //
        // http://docs.datastax.com/en/developer/java-driver/3.1/manual/speculative_execution/
        //
        
        cluster = Cluster
                .builder()
                .addContactPoint("n01.cl01")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)   
                .build();
        session = cluster.connect("brlav35");
        
        
        
        
        

        // Clean up the connection by closing it
        cluster.close();

    }
}
