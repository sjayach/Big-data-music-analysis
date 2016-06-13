package edu.csula.datascience.acquisition;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import twitter4j.Status;
import edu.csula.datascience.models.Track;

/**
 * A test case to show how to use Collector and Source
 */
public class CollectorTest {
	private Collector<Track, Status> collector;
	private Source<Status> source;

	@Before
	public void setup() {
		collector = new TwitterCollector();
		source = new MockSource();
	}

	@Test
	public void mungee() throws Exception {
		List<Track> list = (List<Track>) collector.mungee(source.next());
		// make expected List here
		List<Track> expectedList = new ArrayList<Track>();
		Track track1 = new Track();
		Track track2 = new Track();
		track1.setTrackName("Lemon Tree");
		track1.setArtistName("Fools Garden");
		track2.setTrackName("Better Place");
		track2.setArtistName("Rachel Platten");
		expectedList.add(track1);
		expectedList.add(track2);
		Assert.assertEquals(list.size(), 2);
		for (int i = 0; i < 2; i++) {
			Assert.assertEquals(list.get(i).getTrackName().trim(), expectedList
					.get(i).getTrackName());
			Assert.assertEquals(list.get(i).getArtistName().trim(),
					expectedList.get(i).getArtistName());
		}
	}
}