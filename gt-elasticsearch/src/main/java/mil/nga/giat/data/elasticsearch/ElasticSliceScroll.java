package mil.nga.giat.data.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;
import org.geotools.util.logging.Logging;

import java.io.*;
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

    private List<InputStream> inputStreams;

    private String docType;

    private ElasticRequest elasticRequest;

    private Integer id;

    public ElasticSliceScroll(ElasticDataStore dataStore, String docType, ElasticRequest elasticRequest) {
        this.dataStore = dataStore;
        this.inputStreams = new ArrayList<>();
        this.docType = docType;
        this.elasticRequest = elasticRequest;
        this.id = elasticRequest.getSliceId();
    }

    public List<InputStream> getInputStreams() {
        return inputStreams;
    }

    private String getScrollId(String str) {
        String regex = "(?<=(\"_scroll_id\":\")).*?(?=(\"))";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        matcher.find();
        return matcher.group().trim();
    }

    private int getScrollTimes(String str) {
        String regex = "(?<=(\"value\":)).*?(?=(,))";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        matcher.find();
        int totalNum = Integer.parseInt(matcher.group().trim());
        return totalNum / dataStore.getScrollSize().intValue();
    }

    private String InputStreamToString(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString("UTF-8");
    }


    @Override
    public void run() {
        try {
            LOGGER.fine(id + "---slice start");
            Response rep = dataStore.getClient().searchWithoutParse(dataStore.getIndexName(), docType, elasticRequest);
            InputStream inputStream = rep.getEntity().getContent();
            String content = InputStreamToString(inputStream);
            ByteArrayInputStream stream = new ByteArrayInputStream(content.getBytes());
            inputStreams.add(stream);
            LOGGER.fine(id + "---Search response");

            String scrollId = getScrollId(content);
            int scrollTimes = getScrollTimes(content);
            for (int i = 0; i < scrollTimes; ++i) {
                Response response = dataStore.getClient().scrollWithoutParse(scrollId, dataStore.getScrollTime());
                inputStreams.add(response.getEntity().getContent());
            }
            LOGGER.fine(id + "---slice end");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
