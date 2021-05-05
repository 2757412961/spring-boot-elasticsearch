package org.zjh.esapi.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author: zjh
 * @date : 2021/5/4 19:29
 * @Email : 2757412961@qq.com
 * @update:
 */

@Configuration
public class ElasticSearchConfig {

    @Bean
    public RestHighLevelClient restHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.2.123", 9200, "http"),
                        new HttpHost("localhost", 9201, "http"))
        );

        return client;
    }


}
