package edu.csula.datascience.acquisition;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import edu.csula.datascience.models.AudioProperties;
import edu.csula.datascience.models.Track;

/**
 * An example of Source implementation using Twitter4j api to grab tweets
 */
public class TwitterSource implements Source<Status> {
	List<Status> finalList = new ArrayList<Status>();
	long minId;
	private final String searchQuery;
	ConfigurationBuilder cb;
	TwitterFactory tf;
	Twitter twitter;
	int count = 0;
	int i = 0;
	FileWriter fw;

	public TwitterSource(long minId, String query) throws IOException {
		this.minId = minId;
		this.searchQuery = query;
		
	}

	@Override
	public boolean hasNext() {
		return minId > 0;
	}

	@Override
	public Collection<Status> next() {
		System.out.println("Current MINID: "+minId);
		cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey(Constants.TWITTER_CONSUMER_KEY[i])
				.setOAuthConsumerSecret(Constants.TWITTER_CONSUMER_SECRET[i])
				.setOAuthAccessToken(Constants.TWITTER_ACCESS_TOKEN[i])
				.setOAuthAccessTokenSecret(Constants.TWITTER_ACCESS_SECRET[i]);
		tf = new TwitterFactory(cb.build());
		twitter = tf.getInstance();

		// twitter query making to fetch tweets

		Query query = new Query(searchQuery);
		query.setSince("2016-01-01");
		query.setCount(100);
		query.setLocale("en-US");
		if (minId != Long.MAX_VALUE) {
			query.setMaxId(minId);
		}
		finalList = getTweets(twitter, query);
		System.out.println("Final List size is:"+finalList.size());
		return finalList;
	}

	private List<Status> getTweets(Twitter twitter, Query query) {
		QueryResult result;

		try {
			do {
				int count=1;
				if(count==6){
					break;
				}
				result = twitter.search(query);
				List<Status> tweets = result.getTweets();
				System.out.println("Inside getTweets tweets size ="+tweets.size());
				for(Status status:tweets){
					minId=status.getId();
				}
				finalList.addAll(tweets);
				count++;
			} while ((query = result.nextQuery()) != null);

		} catch (TwitterException e) {
			e.printStackTrace();
			i++;
			System.out.println("For i" + i + " remaining time is"
					+ e.getRateLimitStatus().getSecondsUntilReset());
			if (i == 6) {
				try {
					Thread.sleep(e.getRateLimitStatus().getSecondsUntilReset() * 1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				i = 0;
				next();
			}
		}
		
		return finalList;

	}

	public Collection<Track> fetchSpotify(Collection<Track> cleanedTweets) throws URISyntaxException, IOException {
		// Fetch the tracks information from spotify like id
		// using that id fetch the track properties lusing this https://api.spotify.com/v1/audio-features/{id}
		//Note you can fetch audio-features of 100 track in one api request
		//after this set the properties like track.setAudioProperties
		System.out.println("Spotify has started fetching data");
		FileWriter writer=new FileWriter("./spotify-data.json",true);
		List<JSONArray> spotifyCollection=new ArrayList<JSONArray>();
		List<Track> tracksCollection=new ArrayList<Track>();
		for(Track track:cleanedTweets){
			
			String name=track.getTrackName().trim();
			String artist=track.getArtistName().trim();
			System.out.println("Track name:"+name+"\t artist:"+artist);
			URI uri = new URI("https", 
        			"api.spotify.com", 
        			"/v1/search",
        			"q=track:\""+name+"\" artist:"+artist+"&type=track&limit=20", null);

			//String url="https://api.spotify.com/v1/search?q=track:What's Best For You artist:trey&limit=1&type=track";
			
			
			JsonNode response=null;	
				try {
			            
						response = Unirest.post("https://accounts.spotify.com/api/token?grant_type=authorization_code&code=BQA7AnAwM50lR1qUfQPqou5A6lMn8ESc7yXiJhmJ7YDzC36em_SzUT8aiod9ceKUZ4sONzdygzX-Q5oefNXQ0BTVsb9cmGOd0xf7wEolK1r7ZJloBzpEqGbDMMNopshR7NEonCx2cHn0sFiEypdfcNw3NPsd6TU&redirect_uri=https://www.google.com")
			                .header("Content-Type", "application/json")
			                .header("accept", "application/json")
			                .asJson()
			                .getBody();
						org.json.JSONObject obj=response.getObject();
						System.out.println("******************************"+obj.toString());
						JSONObject tracks=null;
						try{
						 tracks=obj.getJSONObject("tracks");
						}catch(Exception e){
							try{
								Thread.sleep(500);
							}catch(Exception ex){
								ex.printStackTrace();
							}
							continue;
						}
						int total=tracks.getInt("total");
						if(total>0){
						JSONArray results=tracks.getJSONArray("items");
						spotifyCollection.add(results);
						JSONObject trackInfo=results.getJSONObject(0);
						String trackId=trackInfo.getString("id");
						long trackDuration=trackInfo.getLong("duration_ms");
						int popularity=trackInfo.getInt("popularity");
						track.setTrackId(trackId);
						track.setTrackDuration(trackDuration);
						track.setTrackSpotifyPopularity(popularity);
						if(spotifyCollection.size()==1500){
							writer.flush();
							writer.append(spotifyCollection.toString());
							
							spotifyCollection=new ArrayList<JSONArray>();
							
						}
						tracksCollection.add(track);
						}
			        } catch (UnirestException e) {
			            
			            try {
							Thread.sleep(800);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
			        }
				
		}
		if(spotifyCollection.size()>0){
			
		writer.append(spotifyCollection.toString()+"\n");
		
		}
		writer.flush();
		writer.close();
		System.out.println("Spotify is Closing and givig data to Main");
		//write the data present in spotify collection before sending it to main
		
		return tracksCollection;
	}

	public Collection<Track> fetchAudioProperties(Collection<Track> fetchSongs) throws IOException {
		FileWriter fw=new FileWriter("./audio-properties.json",true);
		// TODO Auto-generated method stub
		System.out.println("******* Fetching Spotify Properties ***********");
		
		ArrayList<Track> fetchedSongs=(ArrayList<Track>) fetchSongs;
		int totalSize=fetchedSongs.size();
		int count=Math.max(totalSize/100, 1);
		System.out.println("Count Value is:"+count);
		for(int i=0;i<count;i++){
			String append="";
			int j=100*i;
			for(;j<(100*i)+100 && j<fetchedSongs.size();j++){
				
				Track track=fetchedSongs.get(j);
				append+=track.getTrackId();
				if(j==fetchedSongs.size()-1){
					break;
				}
				append+=",";
			}
			
			
			String urlForAccessToken="https://accounts.spotify.com/api/token?grant_type=client_credentials&client_id=b6e6a0a1fe5344b195c293ee006efa34&client_secret=5ab2738f61ee40868c9aec68e8077a4c";
			JsonNode response=null;
			try{
				response = Unirest.get(urlForAccessToken)
		                .header("Content-Type", "application/json")
		                .header("accept", "application/json")
		                .asJson()
		                .getBody();
					org.json.JSONObject obj=response.getObject();
					System.out.println("Audio Properties Response: ");
					System.out.println(obj.toString());
			}catch(UnirestException e){
				e.printStackTrace();
			}
			
			String url="https://api.spotify.com/v1/audio-features/?ids="+append+"&access_token=BQDHGTxW5N5RKOG24l_U9T--WQfjkgjeQKK_vTs21aY3yV7s9AV_e0g9F-AOYf8k1-AjtI1q4urFljltN3rIO4NBKxJjMACRbeNVO-cQsiR5JLw4sdevQk7otYW-k6L72KOBow";
				
				try {
			            
						response = Unirest.get(url)
			                .header("Content-Type", "application/json")
			                .header("accept", "application/json")
			                .asJson()
			                .getBody();
						org.json.JSONObject obj=response.getObject();
						System.out.println(obj.toString());
						JSONArray audio_features=obj.getJSONArray("audio_features");
						System.out.println("Array Size:"+audio_features.length());
						for(int s=0;i<fetchedSongs.size();s++){
							JSONObject audio=audio_features.getJSONObject(s);
							
							double loudness=audio.getDouble("loudness");
							double liveness=audio.getDouble("liveness");
							double tempo=audio.getDouble("tempo");
							double valence=audio.getDouble("valence");
							double instrumentalness=audio.getDouble("instrumentalness");
							double danceability=audio.getDouble("danceability");
							double speechiness=audio.getDouble("speechiness");
							double mode=audio.getDouble("mode");
							double acousticness=audio.getDouble("acousticness");
							double energy=audio.getDouble("energy");
							Track track=fetchedSongs.get(s);
							AudioProperties ap=new AudioProperties();
							ap.setAcousticness(acousticness);
							ap.setDanceability(danceability);
							ap.setEnergy(energy);
							ap.setInstrumentalness(instrumentalness);
							ap.setLiveness(liveness);
							ap.setLoudness(loudness);
							ap.setMode(mode);
							ap.setSpeechiness(speechiness);
							ap.setTempo(tempo);
							ap.setValence(valence);
							track.setAudioProperties(ap);
							
						}
						
			        } catch (UnirestException e) {
			            
			            try {
							Thread.sleep(800);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
			        }
		}
		for(Track track:fetchedSongs){
		JSONObject obj=new JSONObject();
		obj.put(track.getTrackId(), track.getAudioProperties());
		fw.append(obj.toString());
		}
		fw.flush();
		fw.close();
		return fetchSongs;
	}

}
