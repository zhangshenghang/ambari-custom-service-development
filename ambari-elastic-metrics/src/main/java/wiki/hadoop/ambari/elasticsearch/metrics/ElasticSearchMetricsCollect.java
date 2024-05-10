package wiki.hadoop.ambari.elasticsearch.metrics;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.system.SystemUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * ElasticSearch 性能指标监控获取
 *
 * @author Jast
 * @description
 * @date 2024-03-26 11:40
 */
public class ElasticSearchMetricsCollect {
    private static final Logger LOGGER = Logger.getLogger(ElasticSearchMetricsCollect.class.getName());
    static String esPidFile;
    static String esClusterStatsApi;
    static String esClusterHealthApi;
    static String metricsCollectorApi;

    static String password = "";

    public static void main(String[] args) {

        if (SystemUtil.getOsInfo().isMac()) {
            //paramsInit("{\"es\":{\"ip\":\"bigdata-01\",\"port\":9200,\"pid_file\":\"\",\"log_dir\":\"\"},\"metrics_collector\":{\"ip\":\"bigdata-03\",\"port\":\"6188\"}}");
            paramsInit("{\"metrics_collector\": {\"ip\": \"bigdata-03\", \"port\": \"6188\"}, \"es\": {\"pid_file\": \"/var/run/elasticsearch/elasticsearch.pid\", \"ip\": \"bigdata-01\", \"port\": \"9200\", \"log_dir\": \"/var/log/elasticsearch\"}}");
            run();
        } else {
            if (args.length == 0) {
                System.out.println("请传入参数");
                System.exit(0);
            }
            if (args.length == 2) {
                //集群已启用密码
                password =args[1];
            }
            byte[] decode = Base64.getDecoder().decode(args[0]);
            String param = new String(decode, StandardCharsets.UTF_8);
            paramsInit(param);
            run();
        }
    }

    public static void paramsInit(String paramsData) {
        JSONObject paramsJson = JSON.parseObject(paramsData);

        // 解析
        String mcIp = paramsJson.getJSONObject("metrics_collector").getString("ip");
        int mcPort = paramsJson.getJSONObject("metrics_collector").getIntValue("port");

        esPidFile = paramsJson.getJSONObject("es").getString("pid_file");
        String esIp = paramsJson.getJSONObject("es").getString("ip");
        int esPort = paramsJson.getJSONObject("es").getIntValue("port");
        String esLogDir = paramsJson.getJSONObject("es").getString("log_dir");

        String elasticLogDir = esLogDir;
        String metricsLogFile = "es_metrics.log";
        String metricsLog = elasticLogDir + "/" + metricsLogFile;

            esClusterStatsApi = "http://" + esIp + ":" + esPort + "/_cluster/stats";
            esClusterHealthApi = "http://" + esIp + ":" + esPort + "/_cluster/health";
            metricsCollectorApi = "http://" + mcIp + ":" + mcPort + "/ws/v1/timeline/metrics";


        // 打印结果，或进行其他操作
        System.out.println("esClusterStatsApi: " + esClusterStatsApi);
        System.out.println("esClusterHealthApi: " + esClusterHealthApi);
        System.out.println("metricsCollectorApi: " + metricsCollectorApi);
    }

    public static void sendData() {
        try {
            // 获取集群统计信息
            String statsResponse = HttpRequest.get(esClusterStatsApi)
                    .header("Authorization","Basic "+password)
                    .execute().body();
            JSONObject statsContent = JSON.parseObject(statsResponse);
            System.out.println(statsContent);
            long indicesCount = statsContent.getJSONObject("indices").getLongValue("count");
            long heapUsedMemory = statsContent.getJSONObject("nodes").getJSONObject("jvm").getJSONObject("mem").getLongValue("heap_used_in_bytes");
            long heapMaxMemory = statsContent.getJSONObject("nodes").getJSONObject("jvm").getJSONObject("mem").getLongValue("heap_max_in_bytes");
            int nodesNumber = statsContent.getJSONObject("_nodes").getIntValue("successful");
            int totalNodesNumber = statsContent.getJSONObject("_nodes").getIntValue("total"); // 同上
            // 注意：fastjson2可能没有直接提供获取嵌套JSON对象属性的方法，你可能需要自行封装或者链式调用getJSONObject
            float nodesMemPercent = statsContent.getJSONObject("nodes").getJSONObject("os").getJSONObject("mem").getFloat("used_percent");

            // 假设你已经实现了sendMetricToCollector方法
            sendMetricToCollector("indices.count", indicesCount);
            sendMetricToCollector("heap.used.memory", heapUsedMemory);
            sendMetricToCollector("heap.max.memory", heapMaxMemory);
            sendMetricToCollector("nodes.number", nodesNumber);
            sendMetricToCollector("nodes.total", totalNodesNumber);
            sendMetricToCollector("nodes.mem.percent", nodesMemPercent);

            // 获取集群健康信息
            String healthResponse = HttpRequest.get(esClusterHealthApi)
                    .header("Authorization","Basic "+password)
                    .execute().body();
            JSONObject healthContent = JSON.parseObject(healthResponse);
            int unassignedShards = healthContent.getIntValue("unassigned_shards");

            // 打印或处理unassignedShards
            sendMetricToCollector("unassigned.shards", unassignedShards);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendMetricToCollector(String metricName, Object metricData) {
        long millisTime = System.currentTimeMillis();
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.severe("Failed to get hostname: " + e.getMessage());
        }
        hostname = "bigdata-01";

        Map<String, Object> singleMetric = new HashMap<>();
        singleMetric.put(String.valueOf(millisTime), metricData);

        Map<String, Object> metricsMap = new HashMap<>();
        metricsMap.put("metricname", metricName);
        metricsMap.put("appid", "elasticsearch");
        metricsMap.put("hostname", hostname);
        metricsMap.put("timestamp", millisTime);
        metricsMap.put("starttime", millisTime);
        metricsMap.put("metrics", singleMetric);

        ArrayList<Map<String, Object>> metricsList = new ArrayList<>();
        metricsList.add(metricsMap);

        JSONObject metricsJson = new JSONObject();
        metricsJson.put("metrics", metricsList);

        LOGGER.info(String.format("[%s] send metrics to collector data: %s", metricName, metricsJson.toJSONString()));

        HttpResponse response = HttpRequest.post(metricsCollectorApi)
                .body(metricsJson.toJSONString())
                .header("Content-Type", "application/json")
                .execute();

        int statusCode = response.getStatus();
        String responseBody = response.body();
        if (statusCode == 200) {
            LOGGER.info("send metrics result: " + responseBody);
        } else {
            LOGGER.severe("send metrics failure, status code: " + statusCode + ", response: " + responseBody);
        }
    }

    public static void run() {
        while (checkProcess()) {
            sendData();
            try {
                Thread.sleep(1000 * 10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Boolean checkProcess() {

        boolean exist = FileUtil.exist(esPidFile);
        if (StrUtil.isNotBlank(esPidFile) && exist) {
            String processId = FileUtil.readString(esPidFile, StandardCharsets.UTF_8);
            if (exist && NumberUtil.isNumber(processId)) {
                String result = RuntimeUtil.execForStr("ps -p " + processId);
                boolean processExists = StrUtil.contains(result, String.valueOf(processId));
                return processExists;
            } else {
                return true;
            }
        } else {
            return true;
        }
    }

}
