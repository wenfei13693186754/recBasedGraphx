package spark.client;

import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.client.AsyncRestTemplate;

/**
 * @author CHENYB
 * @since 2016年10月18日
 */
public class SparkSpringRest{  

	String unitServerUrl = "http://192.168.6.84:8090/jobs?appName=startRec1&classPath=com.wdcloud.graphx.jobServer.RecBasedGraphxServer";

	/**
	 * @author CHENYB
	 * @param args
	 * @since 2016年10月18日 下午6:21:40
	 */
	public static void main(String[] args) {
		Properties p = new Properties();
		p.put("user_behavior_table", "USER_BEHAVIOR_TEST");  
		p.put("user_conf_table", "T_USER_CONF");
		p.put("user_scene_id", "JANUARYRRT_TEST");
		p.put("user_business_id", "REC_JOBSERVER");
		p.put("rec_recommender_class", "all");
		p.put("namespace", "JANUARY");
		p.put("rec_result_table", "REC_TEST_RESULT");
		p.put("rec_saveReslut_type", "save_kafka");
		p.put("dataSource_type", "hbase_edge");
		SparkSpringRest rest = new SparkSpringRest();
		rest.invokSparkBySpringRestWithoutResult(p);
	}

	/**
	 * @author CHENYB
	 * @param serverUrl                                                
	 * @since 2016年10月19日 下午2:06:14
	 */
	public void invokSparkBySpringRestWithoutResult(Properties p) {
		AsyncRestTemplate asyncRestTemplate = new AsyncRestTemplate();// 声明spring异步调用模板，spring调用rest接口的类
		HttpHeaders headers = new HttpHeaders();// 创建一个请求头对象
		// MediaType
		MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
		headers.setContentType(type);
		headers.add("Accept", MediaType.APPLICATION_JSON.toString());
		// Authorization
		String plainCreds = "admin:admin";
		byte[] plainCredsBytes = plainCreds.getBytes();
		byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
		String base64Creds = new String(base64CredsBytes);
		headers.add("Authorization", "Basic " + base64Creds);

		HttpEntity<Properties> formEntity = new HttpEntity<Properties>(p, headers);
		ListenableFuture<ResponseEntity<Void>> forEntity = asyncRestTemplate
				.postForEntity(unitServerUrl, formEntity, Void.class);
		// 异步调用后的回调函数
		forEntity
				.addCallback(new ListenableFutureCallback<ResponseEntity<Void>>() {
					// 调用失败
					@Override
					public void onFailure(Throwable e) {
						System.out.println("=====rest response faliure======");
						e.printStackTrace();
					}

					// 调用成功
					@Override
					public void onSuccess(ResponseEntity<Void> result) {
						System.out
								.println("--->async rest response success----, result = "
										+ result);
					}
				});
	}

}
