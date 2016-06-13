package edu.csula.datascience.models;



import com.mongodb.BasicDBList;

public class Track {
	String trackId;
	String trackName;
	String artistName;
	long trackDuration;
	int trackSpotifyPopularity;
	String trackDate;
	int tweetCount;
	AudioProperties audioProperties;  
	//List<Tweet> tweetInfo;
	BasicDBList tweetInfo;
	public long getTrackDuration() {
		return trackDuration;
	}
	public void setTrackDuration(long trackDuration) {
		this.trackDuration = trackDuration;
	}
	public String getTrackId() {
		return trackId;
	}
	public void setTrackId(String trackId) {
		this.trackId = trackId;
	}
	
	public int getTrackSpotifyPopularity() {
		return trackSpotifyPopularity;
	}
	public void setTrackSpotifyPopularity(int trackSpotifyPopularity) {
		this.trackSpotifyPopularity = trackSpotifyPopularity;
	}
	public String getTrackName() {
		return trackName;
	}
	public void setTrackName(String trackName) {
		this.trackName = trackName;
	}
	
	/*public List<Tweet> getTweetInfo() {
		return tweetInfo;
	}
	public void setTweetInfo(List<Tweet> tweetInfo) {
		this.tweetInfo = tweetInfo;
	}*/
	
	public String getArtistName() {
		return artistName;
	}
	public BasicDBList getTweetInfo() {
		return tweetInfo;
	}
	public void setTweetInfo(BasicDBList tweetInfo) {
		this.tweetInfo = tweetInfo;
	}
	public void setArtistName(String artistName) {
		this.artistName = artistName;
	}
	public AudioProperties getAudioProperties() {
		return audioProperties;
	}
	public void setAudioProperties(AudioProperties audioProperties) {
		this.audioProperties = audioProperties;
	}
	public String getTrackDate() {
		return trackDate;
	}
	public void setTrackDate(String trackDate) {
		this.trackDate = trackDate;
	}
	public int getTweetCount() {
		return tweetCount;
	}
	public void setTweetCount(int count) {
		this.tweetCount = count;
	}
	
	
	
	
	
	
	
}
