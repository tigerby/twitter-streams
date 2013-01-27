import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bryan</a>
 * @version 1.0
 */
public class PropertyTest {
    @Test
    public void testLoadProperty() throws IOException {
        Properties prop = new Properties();
        InputStream is = getClass().getResourceAsStream("/config.properties");
        prop.load(is);

        String word = prop.getProperty("word.ignore");

    }

    @Test
    public void testArraySetPerformance() throws IOException {
        String[] words = new String[1000000];

        for(int i=0; i<1000000; i++) {
            words[i] = String.valueOf(i);
        }


        System.out.println("for start time: " + System.currentTimeMillis());
        for(String w: words) {
            if(w.equals("a")) {}
        }
        System.out.println("for end time: " + System.currentTimeMillis());

        System.out.println("set start time: " + System.currentTimeMillis());
        Set set = new HashSet(Arrays.asList(words));
        set.contains("a");
        System.out.println("set end time: " + System.currentTimeMillis());
    }
}
