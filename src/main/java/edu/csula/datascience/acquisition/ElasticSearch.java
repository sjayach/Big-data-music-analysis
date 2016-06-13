package edu.csula.datascience.acquisition;



import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import edu.csula.datascience.models.Track;

import org.bson.Document;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * A quick elastic search example app
 *
 * It will parse the csv file from the resource folder under main and send these
 * data to elastic search instance running locally
 *
 * After that we will be using elastic search to do full text search
 *
 * gradle command to run this app `gradle esExample`
 */
public class ElasticSearch{
	List<Track> tracks=new ArrayList<Track>();
	private final static String indexName = "tracks";
    private final static String typeName = "track";
    Node node;
    public Client client;
	public ElasticSearch(Collection<Track> tracks) {
		// establish database connection to MongoDB
		
		// select collection by name `tweets`
		this.tracks=(ArrayList<Track>) tracks;
		 node= nodeBuilder().settings(Settings.builder()
		            .put("cluster.name", "my-application5")
		            .put("path.home", "elasticsearch-data")).node();
		 client = node.client();
		 
	}
	
    
	public void saveElastic() throws IOException{
		
		 Gson gson = new Gson();
		 XContentBuilder obj = null;
		
				 
				 
				 
				 //root

		 BulkProcessor bulkProcessor = BulkProcessor.builder(
		            client,
		            new BulkProcessor.Listener() {
		                @Override
		                public void beforeBulk(long executionId,
		                                       BulkRequest request) {
		                	System.out.println("Before bulk Operation:"+request.numberOfActions());
		                }

		                @Override
		                public void afterBulk(long executionId,
		                                      BulkRequest request,
		                                      BulkResponse response) {
		                	System.out.println("Bulk finish");
		                }

		                @Override
		                public void afterBulk(long executionId,
		                                      BulkRequest request,
		                                      Throwable failure) {
		                    System.out.println("Facing error while importing data to elastic search");
		                    failure.printStackTrace();
		                }
		            })
		            .setBulkActions(this.tracks.size())
		            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
		            .setFlushInterval(TimeValue.timeValueSeconds(5))
		            .setConcurrentRequests(1)
		            .setBackoffPolicy(
		                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
		            .build();

		for(Track track:tracks){
			 try {
				 obj=XContentFactory.jsonBuilder().startObject()
						 .field("trackId").value(track.getTrackId())
						 .field("trackName").value(track.getTrackName())
						 .field("artistName").value(track.getArtistName())
						 .field("duration").value(track.getTrackDuration())
						 .field("popularity").value(track.getTrackSpotifyPopularity())
						 .field("date").value(track.getTrackDate())
						 /*.field("tweetCount").value(track.getTweetCount())*/
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
						.endObject();
				 		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			bulkProcessor.add(new IndexRequest(indexName, typeName,track.getTrackId()).source(obj.string())); 

			 
		}
		client.close();
		node.close();
		
	}
  
        // Gson library for sending json to elastic search
       
		
      


 
        
       
        	
        	
        
       
    }

    


