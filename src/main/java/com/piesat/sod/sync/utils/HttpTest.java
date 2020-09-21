package com.piesat.sod.sync.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * test
 *
 * @author cwh
 * @date 2020年 08月03日 17:10:31
 */
@Slf4j
public class HttpTest {
    public static void main(String[] args) throws Exception {
        String httpUrl = "https://10.41.15.83:8088/deviceManager/v1/rest/xxxxx/sessions";
        Map<String,Object> m = new HashMap<>();
        m.put("username","admin");
        m.put("password","DM@beijing");
        m.put("scope",0);
        Map<String, Object> headers=new HashMap<String, Object>();
        headers.put("appid", "mm");
        postForAPP(httpUrl,"",m,headers);

    }


    //请求超时时间,这个时间定义了socket读数据的超时时间，也就是连接到服务器之后到从服务器获取响应数据需要等待的时间,发生超时，会抛出SocketTimeoutException异常。
    private static final int SOCKET_TIME_OUT = 60000;
    //连接超时时间,这个时间定义了通过网络与服务器建立连接的超时时间，也就是取得了连接池中的某个连接之后到接通目标url的连接等待时间。发生超时，会抛出ConnectionTimeoutException异常
    private static final int CONNECT_TIME_OUT = 60000;
    private static List<NameValuePair> createParam(Map<String, Object> param) {
        //建立一个NameValuePair数组，用于存储欲传送的参数
        List<NameValuePair> nvps = new ArrayList <NameValuePair>();
        if(param != null) {
            for(String k : param.keySet()) {
                nvps.add(new BasicNameValuePair(k, param.get(k).toString()));
            }
        }
        return nvps;
    }

    /**
     * 发送  post 请求
     * @param url 请求地址，如 http://www.baidu.com
     * @param param相关参数, 模拟form 提交
     * @return
     * @throws Exception
     */
    public static String postForAPP(String url, String sMethod, Map<String, Object> param, Map<String, Object> headers) throws Exception {
        //目前HttpClient最新版的实现类为CloseableHttpClient
        CloseableHttpClient client = SSLClient.sslClient();
        CloseableHttpResponse response = null;
        HttpEntity entity=null;
        try {
            if(param != null) {
                //建立Request的对象，一般用目标url来构造，Request一般配置addHeader、setEntity、setConfig
                HttpPost req = new HttpPost(url);
                entity=new UrlEncodedFormEntity(createParam(param), Consts.UTF_8);
                //setHeader,添加头文件
                Set<String> keys = headers.keySet();
                for (String key : keys) {
                    req.setHeader(key, headers.get(key).toString());
                }
                //setConfig,添加配置,如设置请求超时时间,连接超时时间
                RequestConfig reqConfig = RequestConfig.custom().setSocketTimeout(SOCKET_TIME_OUT).setConnectTimeout(CONNECT_TIME_OUT).build();
                req.setConfig(reqConfig);
                //setEntity,添加内容
                req.setEntity(entity);
                //执行Request请求,CloseableHttpClient的execute方法返回的response都是CloseableHttpResponse类型
                //其常用方法有getFirstHeader(String)、getLastHeader(String)、headerIterator（String）取得某个Header name对应的迭代器、getAllHeaders()、getEntity、getStatus等
                response = client.execute(req);
                entity =  response.getEntity();
                //用EntityUtils.toString()这个静态方法将HttpEntity转换成字符串,防止服务器返回的数据带有中文,所以在转换的时候将字符集指定成utf-8就可以了
                String result= EntityUtils.toString(entity, "UTF-8");
                log.error("-------------------------"+result+"-------------");
                if(response.getStatusLine().getStatusCode()==200){
                    log.error(result+"-----------success------------------");
                    return result;
                }else{
                    log.error(response.getStatusLine().getStatusCode()+"------------------fail-----------");
                    return null;
                }
            }
            return null;
        } catch(Exception e) {
            log.error("--------------------------post error: ", e);
            throw new Exception();
        }finally{
            //一定要记得把entity fully consume掉，否则连接池中的connection就会一直处于占用状态
            EntityUtils.consume(entity);
            log.error("---------------------------finally-------------");
            System.out.println("---------------------------------------------------");
        }
    }

}
