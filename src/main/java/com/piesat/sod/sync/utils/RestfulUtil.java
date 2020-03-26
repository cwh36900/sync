package com.piesat.sod.sync.utils;

import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class RestfulUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestfulUtil.class);
    private static final MediaType Content_Type = MediaType.parse("application/json;charset=utf-8");
    private final static int READ_TIMEOUT = 100;

    private final static int CONNECT_TIMEOUT = 60;

    private final static int WRITE_TIMEOUT = 60;

    private static volatile OkHttpClient okHttpClient;

    private RestfulUtil(){
        okhttp3.OkHttpClient.Builder clientBuilder = new okhttp3.OkHttpClient.Builder();
        //读取超时
        clientBuilder.readTimeout(READ_TIMEOUT, TimeUnit.SECONDS);
        //连接超时
        clientBuilder.connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS);
        //写入超时
        clientBuilder.writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS);
        //自定义连接池最大空闲连接数和等待时间大小，否则默认最大5个空闲连接
        clientBuilder.connectionPool(new okhttp3.ConnectionPool(100,1,TimeUnit.MINUTES));

        okHttpClient = clientBuilder.build();
    }

    public static OkHttpClient getInstance(){
        if (null == okHttpClient){
            synchronized (RestfulUtil.class){
                if (okHttpClient == null){
                    new RestfulUtil();
                    return okHttpClient;
                }
            }
        }
        return okHttpClient;
    }

//    private static final OkHttpClient client = new OkHttpClient();





//    static {
//    	client.setRetryOnConnectionFailure(true);
//    	com.squareup.okhttp.ConnectionPool pool = new com.squareup.okhttp.ConnectionPool(100, 10, TimeUnit.SECONDS);
//    	client.setConnectionPool(pool);
//    }

    public static String Post(String url , String content ){
        Response response = null;
        try {
            LOGGER.debug(" post url {} and param {}" ,url , content);
            RequestBody body = RequestBody.create(Content_Type, content);
            Request request = new Request.Builder().post(body).url(url)
                    .addHeader("content-type", "application/json")
                    .build();
            response = getInstance().newCall(request).execute();
            if (!response.isSuccessful()) throw new IOException("Unexpected code  {} " + response);
            return response.body().string();
        }catch (Exception e){
            LOGGER.error("error {} " , Utils.getExceptionToString(e) );
        }finally {
            if(response != null){
//                try {
                    response.body().close();
//                } catch (IOException e) {
//                    LOGGER.error(" IOException" + Convert.getStackTraceInfo(e));
//                }
            }
        }
        return null;
    }
    public static String Get(String url, Map<String,String> content){
        Response response = null;
        try {
            if (content != null && content.size() != 0) {
                url = url.concat("?");
                for (Map.Entry<String, String> key : content.entrySet()) {
                    url = url.concat(key.getKey()).concat("=").concat(key.getValue()).concat("&");
                }
                url = url.substring(0,url.length()-1);
            }
            LOGGER.info(" Get url {} " ,url);
            Request request = new Request.Builder()
                    .url(url)
                    .build();
            response = getInstance().newCall(request).execute();
            if (!response.isSuccessful()) throw new IOException("Unexpected code  {} " + response);
            return response.body().string();
        }catch (Exception e){
            LOGGER.error("error {} " , Utils.getExceptionToString(e) );
        }finally {
            if(response != null){
//                try {
                    response.body().close();
//                } catch (IOException e) {
//                    LOGGER.error(" IOException" + Convert.getStackTraceInfo(e));
//                }
            }
        }
        return null;
    }
}

