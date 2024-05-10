package wiki.hadoop.ambari.mysql;

import com.alibaba.fastjson2.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jast
 * @description MySQL工具
 * @date 2024-03-08 10:59
 */
public class Tool {

    public static void main(String[] args) throws SQLException {
        Map<String, String> paramMap = paramConvert(args);
        String type = paramMap.get("type");
        switch (type) {
            case "execute":
                execute(paramMap);
                break;
            case "query":
                query(paramMap);
                break;
            default:
                System.out.println("参数错误");
                break;

        }


    }

    public static void query(Map<String, String> paramMap) {
        String uri = paramMap.get("uri");
        String user = paramMap.get("user");
        String password = paramMap.get("password");
        String sql = paramMap.get("sql");
        String url = "jdbc:mysql://" + uri;

        if (password != null) {
            if (password.trim().length() == 0) {
                password = null;
            }
        }
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            List<Map<String, Object>> resultList = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                resultList.add(row);
            }
            System.out.println(JSONObject.toJSONString(resultList));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void execute(Map<String, String> paramMap) {
        String uri = paramMap.get("uri");
        String user = paramMap.get("user");
        String password = paramMap.get("password");
        String sql = paramMap.get("sql");
        String url = "jdbc:mysql://" + uri;

        if (password != null) {
            if (password.trim().length() == 0) {
                password = null;
            }
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();
            int rowsAffected = stmt.executeUpdate(sql);
            System.out.println(rowsAffected);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 参数解析工具类
     *
     * @param args
     * @return java.util.Map<java.lang.String, java.lang.String>
     * @name paramConvert
     * @date 2024/3/11 2:06 PM
     * @author Jast
     */
    public static Map<String, String> paramConvert(String[] args) {
        Map<String, String> paramMap = new HashMap<>();

        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    paramMap.put(parts[0], parts[1]);
                }
            }
        }
        return paramMap;
    }
}
