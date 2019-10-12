package mil.nga.giat.data.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;
import org.geotools.util.logging.Logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private String getScrollId(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[128];
        int length;
        length = inputStream.read(buffer);
        result.write(buffer, 0, length);
        String str = result.toString(StandardCharsets.UTF_8.name());

        String regex = "(?<=(\"_scroll_id\":\")).*?(?=(\"))";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        matcher.find();
        return matcher.group().trim();
    }


    @Override
    public void run() {
        try {
            LOGGER.fine(id + "---slice start");
            //final ElasticResponse sr = dataStore.getClient().search(dataStore.getIndexName(), docType, elasticRequest);
            RestElasticClient restElasticClient = (RestElasticClient) dataStore.getClient();
            Response rep = restElasticClient.search2(dataStore.getIndexName(), docType, elasticRequest);
            responses.add(rep);
            InputStream inputStream = rep.getEntity().getContent();
            String scrollId = getScrollId(inputStream);
            //LOGGER.fine(id + "---Search response: " + sr);
            LOGGER.fine(id + "---Search response: ");

            for (int i = 0; i < 8; ++i) {
                Response response = dataStore.getClient().scrollTest(scrollId, dataStore.getScrollTime());
                responses.add(response);
            }
            LOGGER.fine(id + "---slice end");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
