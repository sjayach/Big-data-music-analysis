package edu.csula.datascience.examples;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import edu.csula.datascience.models.Track;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

public class JestExampleApp {
	List<Track> tracks=new ArrayList<Track>();
	private final static String indexName = "tracks";
    private final static String typeName = "track";
    String awsAddress = "http://search-tracks-27cjasepogshfwyiufy2o2xwii.us-west-2.es.amazonaws.com/";
    JestClientFactory factory;
    public JestClient client;
    public JestExampleApp(List<Track>tracks) {
    	factory = new JestClientFactory();
    	factory.setHttpClientConfig(new HttpClientConfig
    	        .Builder(awsAddress)
    	        .multiThreaded(true)
    	        .build());
    	client = factory.getObject();
    	this.tracks = tracks;
    }
    @SuppressWarnings("deprecation")
	public void saveAWSElastic () throws InterruptedException {
    	try {
            Collection<BulkableAction> actions = Lists.newArrayList();
            XContentBuilder obj = null;
            for(Track track:tracks){
   			 
   				 /*obj=XContentFactory.jsonBuilder().startObject()
   						 .field("trackId").value(track.getTrackId())
   						 .field("trackName").value(track.getTrackName())
   						 .field("artistName").value(track.getArtistName())
   						 .field("duration").value(track.getTrackDuration())
   						 .field("popularity").value(track.getTrackSpotifyPopularity())
   						 .field("date").value(track.getTrackDate())
   				 		.startObject("audioProperties").field("loudness").value(track.getAudioProperties().getLoudness())
   				 		.field("liveness").value(track.getAudioProperties().getLiveness())
   				 		.field("tempo").value(track.getAudioProperties().getTempo())
   				 		.field("valence").value(track.getAudioProperties().getValence())
   				 		.field("instrumentalness").value(track.getAudioProperties().getInstrumentalness())
   				 		.field("danceability").value(track.getAudioProperties().getDanceability())
   				 		.field("speechiness").value(track.getAudioProperties().getSpeechiness())
   				 		.field("mode").value(track.getAudioProperties().getMode())
   				 		.field("acousticness").value(track.getAudioProperties().getAcousticness())
   				 		.field("energy").value(track.getAudioProperties().getEnergy())
   				 		.endObject().startArray("tweetInfo");
   				 		
   				 		for( Object dbObj: track.getTweetInfo()) {
   				 			 obj.value(dbObj);
   				 		}
   				 		
   				 		obj.endArray()
   						.endObject();*/
            	
            	
            	
            	//System.out.println("Date is " + date);
            	
            	System.out.println("Date :" + track.getTrackDate());
            	
            	Gson gson = new Gson();
            	
   				 	System.out.println(gson.toJson(track));
   				 	Index index = new Index.Builder(gson.toJson(track)).index(indexName).type(typeName).id(track.getTrackId()).build();
   				 	
   				 System.out.println("created Index");
   				 try {
   				 client.execute(index);
   				 } catch(Exception e) {
   					 System.out.println("Exception occured" +e);
   					 e.printStackTrace();
   				 }
   				 System.out.println("Execution Done");
   				 //Thread.sleep(10000);
   			            //actions.add(new Index.Builder(obj).build());
            }
   			 
            /*Bulk.Builder bulk = new Bulk.Builder()
                .defaultIndex(indexName)
                .defaultType(typeName)
                .addAction(actions);
            client.execute(bulk.build());*/
            
            System.out.println("Inserted 500 documents to cloud");
        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(10000);
        }
    }
    
 


}
