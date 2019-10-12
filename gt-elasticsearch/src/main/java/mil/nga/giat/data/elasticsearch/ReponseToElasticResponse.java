package mil.nga.giat.data.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

final public class ReponseToElasticResponse implements Runnable{
    private final ObjectMapper mapper;
    private final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private InputStream inputStream;
    private boolean parseDone = false;

    public boolean isParseDone() {
        return parseDone;
    }

    public ElasticResponse getElasticResponse() {
        return elasticResponse;
    }

    private ElasticResponse elasticResponse;

    public ReponseToElasticResponse(InputStream inputStream) {
        this.mapper = new ObjectMapper();
        this.mapper.setDateFormat(DATE_FORMAT);
        this.inputStream = inputStream;
    }

    private ElasticResponse parseResponse() throws IOException {
        return this.mapper.readValue(inputStream, ElasticResponse.class);
    }

    @Override
    public void run(){
        try {
            parseDone = false;
            elasticResponse = parseResponse();
            parseDone = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
