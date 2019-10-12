package mil.nga.giat.data.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;
import org.geotools.util.logging.Logging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ElasticSliceScroll
 *
 * @desc:
 * @author: wuao <wuao@baidu.com>
 * @time: 2019-10-12 09:50
 */
public class ElasticSliceScroll implements Runnable{

    private final static Logger LOGGER = Logging.getLogger(ElasticSliceScroll.class);

    private final ElasticDataStore dataStore;

    private final String scrollId;

    private List<Response> responses;

    public ElasticSliceScroll(ElasticDataStore dataStore,String scrollId) {
        this.dataStore = dataStore;
        this.scrollId = scrollId;
        this.responses = new ArrayList<>();
    }

    public List<Response> getResponses(){
        return responses;
    }


    @Override
    public void run() {
        try {
            /*for (int i = 0; i < 4; ++i) {
                Response response = dataStore.getClient().scrollTest(scrollId, dataStore.getScrollTime());
                responses.add(response);
            }*/
            Response response = dataStore.getClient().scrollTest(scrollId, dataStore.getScrollTime());
            responses.add(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
