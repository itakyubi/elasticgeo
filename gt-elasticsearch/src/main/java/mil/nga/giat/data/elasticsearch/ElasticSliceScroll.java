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
public class ElasticSliceScroll implements Runnable {

    private final static Logger LOGGER = Logging.getLogger(ElasticSliceScroll.class);

    private final ElasticDataStore dataStore;

    private List<Response> responses;

    private String docType;

    private ElasticRequest elasticRequest;

    private Integer id;

    public ElasticSliceScroll(ElasticDataStore dataStore, String docType, ElasticRequest elasticRequest) {
        this.dataStore = dataStore;
        this.responses = new ArrayList<>();
        this.docType = docType;
        this.elasticRequest = elasticRequest;
        this.id = elasticRequest.getSliceId();
    }

    public List<Response> getResponses() {
        return responses;
    }


    @Override
    public void run() {
        try {
            LOGGER.fine(id + "---slice start");
            final ElasticResponse sr = dataStore.getClient().search(dataStore.getIndexName(), docType, elasticRequest);
            LOGGER.fine(id + "---Search response: " + sr);

            for (int i = 0; i < 8; ++i) {
                Response response = dataStore.getClient().scrollTest(sr.getScrollId(), dataStore.getScrollTime());
                responses.add(response);
            }
            LOGGER.fine(id + "---slice end");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
