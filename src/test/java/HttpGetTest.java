import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.Test;
import twitter.streaming.ApiStreamingSpout;

/**
 * Created with IntelliJ IDEA.
 * User: tigerby
 * Date: 12/18/12
 * Time: 3:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class HttpGetTest {
    @Test
    public void testHttpGet() {
        HttpGet get = new HttpGet(ApiStreamingSpout.STREAMING_API_URL+"선거");
        System.out.println(get);
        HttpResponse response;
    }
}
