package org.zjh.esapi.config;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.zjh.esapi.api.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author: zjh
 * @date : 2021/5/4 19:32
 * @Email : 2757412961@qq.com
 * @update:
 */

@SpringBootTest
public class EsTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    // 测试索引的创建
    @Test
    public void testCreateIndex() throws IOException {
        // 1. 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("java_client_index");
        // 2. 客户端执行请求
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse.toString());
    }

    // 测试获取索引
    @Test
    public boolean testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("java_client_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);

        System.out.println(exists);
        return exists;
    }

    // 测试删除索引
    @Test
    public void testDeleteIndex() throws IOException {
        if (!testExistIndex()) {
            return;
        }

        DeleteIndexRequest request = new DeleteIndexRequest("java_client_index");
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);

        System.out.println(acknowledgedResponse.toString());
    }

    // 测试添加文档
    @Test
    public void testCreateDoc() throws IOException {
        IndexRequest request = new IndexRequest("java_client_index");
        User user = new User("qwe", 789);

        request.id("3");
        request.source(JSON.toJSONString(user), XContentType.JSON);

        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());
    }

    // 获取文档，判断是否存在get /index/doc/1
    @Test
    public void testGetDoc() throws IOException {
        GetRequest request = new GetRequest("java_client_index", "3");

        GetResponse documentFields = restHighLevelClient.get(request, RequestOptions.DEFAULT);

        System.out.println(documentFields.toString());
        System.out.println(documentFields);
    }

    // 更新文档的信息
    @Test
    public void testUpdateDoc() throws IOException {
        UpdateRequest request = new UpdateRequest("java_client_index", "1");

        User user = new User("update", 123);
        request.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);

        System.out.println(updateResponse);
        System.out.println(updateResponse.status());
    }

    // 删除文档记录
    @Test
    public void testDeleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("java_client_index", "2");

        DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.toString());
        System.out.println(deleteResponse.status());
    }

    // 特殊的，真的项目一般都会批量插入数据!
    @Test
    public void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        ArrayList<User> users = new ArrayList<>();
        users.add(new User("user001", 1));
        users.add(new User("user002", 2));
        users.add(new User("user003", 333));
        users.add(new User("user004", 4));
        users.add(new User("user005", 5));
        users.add(new User("user006", 6));

        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("java_client_index")
                            .id("" + i)
                            .source(JSON.toJSONString(users.get(i)),
                                    XContentType.JSON)
            );
        }

        BulkResponse bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println(bulkItemResponses);
        System.out.println(bulkItemResponses.status());
        System.out.println(bulkItemResponses.hasFailures());
    }

    // search
    @Test
    public void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("java_client_index");

        // 构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询条件，我们可以使用 QueryBuilders 工具来实现
        // QueryBuilders.termQuery 精确
        // QueryBuilders.matchAllQuery() 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "user005");
        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        request.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);

//        System.out.println(search);
        System.out.println(JSON.toJSONString(search.getHits()));
    }

}
