package edu.osu.library.dspace.statistics;


import org.dspace.content.DSpaceObject;
import org.dspace.eperson.EPerson;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


import javax.servlet.http.HttpServletRequest;

public class ElasticSearchLogger
{

 public static void post(DSpaceObject dso, HttpServletRequest request, EPerson currentUser)
 {


     Client client = new TransportClient()
             .addTransportAddress(new InetSocketTransportAddress("localhost", 9300)) ;



     IndexRequest indexRequest = new IndexRequest();
     indexRequest.type("kb").index("stats").source("{\"user\":\"jim\"}");



     client.prepareIndex();
     client.index(indexRequest);

     client.close();
            

 }

}
