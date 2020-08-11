package com.LatihanConsumerKafka.springbootkafkaconsumerexample.listener;

//import com.LatihanConsumerKafka.springbootkafkaconsumerexample.model.User;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


@Service
public class KafkaConsumer {


    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200)).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
        @Override
        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
            return builder.setConnectionRequestTimeout(5000).setSocketTimeout(1200000);
        }
    });
    RestHighLevelClient highClient = new RestHighLevelClient(builder);

    Date dt = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
    String tgl = sdf.format(dt);
    String[] idxx = tgl.split("-");
    String idx = idxx[0] + "" + idxx[1];

    @KafkaListener(topics = "ipd-data-train", groupId = "test-nasri-0.0.1.2")
    public void consume(@Payload List<String> message) {
        BulkRequest bulkRequest = new BulkRequest();
        int counter = 0;
        for (int a = 0; a < message.size(); a++) {
            JSONObject jsonObject = new JSONObject(message.get(a));
            System.out.println("Message : " + message.get(a));
            System.out.println(message.getClass().getTypeName() + "    " + a);
//            System.out.println(message.size());
            System.out.println("============================");
            bulkRequest.add(new IndexRequest("fas-data-train-hot-" + idx).id(jsonObject.getString("id")).source(message.get(a), XContentType.JSON));
            counter++;
        }
        if (counter >= 100) {
            try {

                BulkResponse bulkResponse = highClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                System.out.println(bulkResponse.status());
            } catch (IOException e) {
                e.getMessage();
            }
            counter = 0;
        }


    }


}
