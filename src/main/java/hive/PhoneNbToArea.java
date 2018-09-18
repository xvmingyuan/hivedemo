package hive;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * hive 自定义函数
 * function: 识别手机归属区域
 * type: 手机号  归属区 
 * eg: 1388677312  beijing	
 * @author xmy
 * @time：2018年7月23日 下午5:30:16
 */
public class PhoneNbToArea extends UDF {
	
	private static ConcurrentHashMap<String, String> areaMap = new ConcurrentHashMap<String, String>();
	static {
		areaMap.put("1388", "beijing");
		areaMap.put("1399", "tianjin");
		areaMap.put("1366", "nanjing");
	}
	public String evaluate(String pnb) {
		String result = areaMap.get(pnb.substring(0, 4))==null?(pnb+"  NotKnow"):(pnb+"  "+areaMap.get(pnb.substring(0, 4)));
		return result;
	}
	
}
