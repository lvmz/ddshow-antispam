package com.youku.ddshow.antispam.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.net.URLDecoder;
import java.util.*;


/**
 * @author wangqiao
 * @date 2014-7-10 下午5:45:52
 */
public class LogUtils {

    private LogUtils() {

    }



    //消费类型信息
    private static Set<Integer> CONSUME_SET = new HashSet<Integer>();

    static {
        //0,2,4,12,14,17,19,25,26,27,29,30,31,32,33
        CONSUME_SET.add(0);
        CONSUME_SET.add(2);
        CONSUME_SET.add(4);
        CONSUME_SET.add(12);
        CONSUME_SET.add(14);
        CONSUME_SET.add(17);
        CONSUME_SET.add(19);
        CONSUME_SET.add(25);
        CONSUME_SET.add(26);
        CONSUME_SET.add(27);
        CONSUME_SET.add(29);
        CONSUME_SET.add(30);
        CONSUME_SET.add(31);
        CONSUME_SET.add(32);
        CONSUME_SET.add(33);
        CONSUME_SET.add(34);
        CONSUME_SET.add(35);
        CONSUME_SET.add(36);
        CONSUME_SET.add(37);
        CONSUME_SET.add(38);
        CONSUME_SET.add(39);
        CONSUME_SET.add(40);
    }

    // clientInfo 信息
    private static enum ClientInfoType {

        CLINET_INFO("clientInfo"), APPID_KEY("appId"), APP_VERSION("appVersion"), OS_VERSION("osVersion"), OS("os"), DEVICE_TOKEN(
                "deviceToken"), CHANNEL_ID("channelId"), MID("mid"), CID("cid"), PID("pid"), LPID("lpid"), CHANNEL("channel");

        private String type;

        ClientInfoType(String type) {
            this.type = type;
        }

        public String getClientInfoType() {
            return type;
        }

    }

    // 爬虫或机器人
    private static enum SpiderType {

        SPIDER_AGENT("spider"), UP_SPIDER_AGENT("Spider"), GOOGLE_BOT("Googlebot"), APACHE_HTTP_BOT("HttpClient"), JAVA_BOT(
                "ava/1"), BOT("bot"), SCRAPY("scrapy"), SIMULATOR("Simulator"), YISOUSPIDER("YisouSpider"), ROBOOBOT(
                "roboobot"), PYTHON_BOT("Python"), APPENGINE_BOT("AppEngine"), SINA_WEIBO_BOT("SinaWeiboBot"), MSN_BOT(
                "msnbot"), WEB_MASTER_BOT("webmasters");

        private String spider;

        SpiderType(String spider) {
            this.spider = spider;
        }

        public String getSpiderType() {
            return spider;
        }

    }

    public static boolean spiderFilter(String userAgent) {
        Boolean flag = Boolean.TRUE;
        // 过滤spider
        if (StringUtils.isNotBlank(userAgent)
                && (userAgent.toLowerCase().indexOf(SpiderType.SPIDER_AGENT.getSpiderType()) > -1
                        || userAgent.indexOf(SpiderType.UP_SPIDER_AGENT.getSpiderType()) > -1 || userAgent
                        .indexOf(SpiderType.GOOGLE_BOT.getSpiderType()) > -1
                /*
                 * || userAgent.indexOf(SpiderType.APACHE_HTTP_BOT.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.JAVA_BOT.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.BOT.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.SCRAPY.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.SIMULATOR.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.YISOUSPIDER.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.ROBOOBOT.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.PYTHON_BOT.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.APPENGINE_BOT.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.SINA_WEIBO_BOT.getSpiderType()) > -1 ||
                 * userAgent.indexOf(SpiderType.MSN_BOT.getSpiderType()) > -1 || userAgent
                 * .indexOf(SpiderType.WEB_MASTER_BOT.getSpiderType()) > -1
                 */)) {
            flag = Boolean.FALSE;
        }
        return flag;
    }

    public static boolean clientInfoFilter(String clientInfo) {
        Boolean flag = Boolean.TRUE;
        if (StringUtils.isBlank(clientInfo) || "{}".equals(clientInfo)) {
            flag = Boolean.FALSE;
        }
        return flag;
    }

    public static boolean jsonArrayInfoFilter(String jsonInfo) {
        Boolean flag = Boolean.TRUE;
        if (StringUtils.isBlank(jsonInfo) && "[]".equals(jsonInfo)) {
            flag = Boolean.FALSE;
        }
        return flag;
    }

    public static enum IEType {
        IE6("ie6"), IE7("ie7"), IE8("ie8"), IE9("ie9"), IE10("ie10"), IE11("ie11"), FF("Firefox"), CHROME("Chrome"), IPHONE(
                "iphone"), IPAD("ipad"), OTHERMOBILE("othermobile"), OTHER("other"), NULL("NULL"), Android("Android");

        private String type;

        IEType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    public static class IEParse {

        public static IEType parse(String values) {
            if (values == null || "".equals(values.trim())) {
                return IEType.NULL;
            }
            if (values.toLowerCase().indexOf("googlebot") > -1 || values.toLowerCase().indexOf("spider") > -1) {
                return IEType.NULL;
            }

            if (values.indexOf("Windows") > -1 && values.indexOf("rv") > -1) {
                return IEType.IE11;
            }

            if (values.indexOf("Linux") > -1 && values.indexOf("Android") > -1) {
                return IEType.Android;
            }

            IEType type = IEType.OTHER;
            if (values.indexOf("MSIE") > -1) {
                if (values.indexOf("MSIE 6.0") > -1) {
                    type = IEType.IE6;
                } else if (values.indexOf("MSIE 7.0") > -1) {
                    type = IEType.IE7;
                } else if (values.indexOf("MSIE 8.0") > -1) {
                    type = IEType.IE8;
                } else if (values.indexOf("MSIE 9.0") > -1) {
                    type = IEType.IE9;
                } else if (values.indexOf("MSIE 10.0") > -1) {
                    type = IEType.IE10;
                } else {
                    type = IEType.IE11;
                }
            } else if (values.indexOf("Firefox") > -1) {
                type = IEType.FF;
            } else if (values.indexOf("Chrome") > -1) {
                type = IEType.CHROME;
            } else if (values.indexOf("iPhone") > -1) {
                type = IEType.IPHONE;
            } else if (values.indexOf("iPad") > -1) {
                type = IEType.IPAD;
            } else if (values.indexOf("Mobile") > -1) {
                type = IEType.OTHERMOBILE;
            } else {
                type = IEType.NULL;
            }

            return type;
        }

    }

    /**
     * 获取roomid
     * 
     * @param str
     * @return
     */
    public static int getRoomId(String str) {
        if (StringUtils.isBlank(str) || str.trim().equalsIgnoreCase("/")) { // 首页
            return 0;
        }

        // 如果连接中没有room
        if (str.indexOf("/room/") < 0) {
            String[] ss = StringUtils.split(str, "/");
            if (null != ss && ss.length >= 1 && StringUtils.isNumeric(ss[0])) {
                return NumberUtils.toInt(ss[0]);
            }
            return 0;
        }

        str = str.trim();
        // 如果连接中有room
        if (str.indexOf("/room/") > -1) {
            String[] arrayStr = StringUtils.split(str, "/");
            if (null != arrayStr && arrayStr.length >= 2) {
                String room = arrayStr[0];
                if (StringUtils.isNotBlank(room) && "room".equals(room)) {
                    return NumberUtils.toInt(arrayStr[1], 0);
                }
            }
        }
        return 0;
    }

    /**
     * 获取参数json
     * 
     * @param params
     * @return
     */
    public static JSONObject getParamsJson(String params) {
        // 参数转为json
        JSONObject paramsJson = null;
        if (StringUtils.isNotBlank(params)) {
            paramsJson = new JSONObject();
            String[] paramsArray = StringUtils.split(params, "&");
            if (null != paramsArray && paramsArray.length > 0) {
                for (String str : paramsArray) {
                    if (StringUtils.isNotBlank(str)) {
                        String[] arrayStr = StringUtils.split(str, "=");
                        if (null != arrayStr && arrayStr.length >= 2) {
                            String keyTemp = arrayStr[0];
                            String valueTemp = arrayStr[1];
                            if (StringUtils.isNotBlank(keyTemp) && StringUtils.isNotBlank(valueTemp)) {
                                paramsJson.put(keyTemp, valueTemp);
                            }
                        }
                    }
                }
            }
            if (null != paramsJson) {
                return paramsJson;
            }
        }
        return paramsJson;
    }

    /**
     * 获取module
     * 
     * @param uri
     * @return
     */
    public static String getModule(String uri) {
        String module = StringUtils.EMPTY;
        if (StringUtils.isNotBlank(uri)) {
            String[] arrayUrl = StringUtils.split(uri, "\\/");
            if (null != arrayUrl && arrayUrl.length > 0) {
                if (arrayUrl.length >= 1 && StringUtils.isNotBlank(arrayUrl[0])) {
                    module = arrayUrl[0];
                }
            }
        }
        return module;
    }

    /**
     * 获取api module
     * 
     * @param uri
     * @return
     */
    public static String getApiModule(String uri) {
        String module = StringUtils.EMPTY;
        if (StringUtils.isNotBlank(uri)) {
            String[] arrayUrl = StringUtils.split(uri, "\\/");
            if (null != arrayUrl && arrayUrl.length > 0) {
                if (arrayUrl.length >= 1 && StringUtils.isNotBlank(arrayUrl[0])) {
                    module = arrayUrl[0];
                    if (StringUtils.isNotBlank(module) && "v1".equals(module) && StringUtils.isNotBlank(arrayUrl[1])) {
                        module = arrayUrl[1];
                    }
                }
            }
        }
        return module;
    }

    /**
     * 获取ugc datajson
     * 
     * @param dataInfo
     * @return
     */
    public static JSONObject getUgcDataJson(String dataInfo) {
        JSONObject dataJson = null;
        if (StringUtils.isNotBlank(dataInfo) && !"{}".equals(dataInfo)) {
            dataJson = new JSONObject();
            JSONObject dataInfoJson = JSONObject.parseObject(dataInfo);
            if (null != dataInfoJson && dataInfoJson.containsKey("dataInfo")) {
                dataJson = (JSONObject) dataInfoJson.get("dataInfo");
            }
        }
        return dataJson;
    }

    /**
     * 获取clientInfo信息
     */
    public static Map<String, String> getClientMap(String clientInfo) {
        Map<String, String> clientMap = null;
        if (StringUtils.isNotBlank(clientInfo) && !"{}".equals(clientInfo)) {
            clientMap = new HashMap<String, String>();
            JSONObject clientInfoJson = JSONObject.parseObject(clientInfo);
            if (null != clientInfoJson && clientInfoJson.containsKey(ClientInfoType.CLINET_INFO.getClientInfoType())) {
                JSONObject clientInfoMap = (JSONObject) clientInfoJson.get(ClientInfoType.CLINET_INFO
                        .getClientInfoType());
                if (null != clientInfoMap && clientInfoMap.size() > 0
                        && clientInfoMap.containsKey(ClientInfoType.APPID_KEY.getClientInfoType())) {
                    // 获取appid
                    String appid = clientInfoMap.getString(ClientInfoType.APPID_KEY.getClientInfoType());
                    if (StringUtils.isNotBlank(appid)) {
                        clientMap.put(ClientInfoType.APPID_KEY.getClientInfoType(), appid);
                        //20151221加入channel的获取
                        if (clientInfoMap.containsKey(ClientInfoType.CHANNEL.getClientInfoType())) {
                            String channel = clientInfoMap.getString(ClientInfoType.CHANNEL.getClientInfoType());
                            if(StringUtils.isNotBlank(channel)){
                                clientMap.put(ClientInfoType.CHANNEL.getClientInfoType(), channel);
                            }
                        }
                        if (clientInfoMap.containsKey(ClientInfoType.APP_VERSION.getClientInfoType())) {
                            String appVersion = clientInfoMap.getString(ClientInfoType.APP_VERSION.getClientInfoType());
                            if (StringUtils.isNotBlank(appVersion)) {
                                clientMap.put(ClientInfoType.APP_VERSION.getClientInfoType(), appVersion);
                            }
                        }
                        if (clientInfoMap.containsKey(ClientInfoType.OS_VERSION.getClientInfoType())) {
                            String osVersionStr = clientInfoMap
                                    .getString(ClientInfoType.OS_VERSION.getClientInfoType());
                            if (StringUtils.isNotBlank(osVersionStr)) {
                                String[] arrayOs = StringUtils.split(osVersionStr, "_");
                                if (null != arrayOs && arrayOs.length >= 2) {
                                    String os = arrayOs[0];
                                    if (StringUtils.isNotBlank(os)) {
                                        clientMap.put(ClientInfoType.OS.getClientInfoType(), os);
                                    }
                                    String osVersion = arrayOs[1];
                                    if (StringUtils.isNotBlank(osVersion)) {
                                        clientMap.put(ClientInfoType.OS_VERSION.getClientInfoType(), osVersion);
                                    }
                                }
                            }
                        }
                        if (clientInfoMap.containsKey(ClientInfoType.DEVICE_TOKEN.getClientInfoType())) {
                            String deviceToken = clientInfoMap.getString(ClientInfoType.DEVICE_TOKEN
                                    .getClientInfoType());
                            if (StringUtils.isNotBlank(deviceToken)) {
                                clientMap.put(ClientInfoType.DEVICE_TOKEN.getClientInfoType(), deviceToken);
                            }
                        }
                        if (clientInfoMap.containsKey(ClientInfoType.CHANNEL_ID.getClientInfoType())) {
                            String channelId = clientInfoMap.getString(ClientInfoType.CHANNEL_ID.getClientInfoType());
                            if (StringUtils.isNotBlank(channelId)) {
                                String[] tmpArray = StringUtils.split(channelId, "_");
                                if (null != tmpArray && tmpArray.length >= 2) {
                                    String cps = tmpArray[1];
                                    if (StringUtils.isNotBlank(cps)) {

                                        // 对cps信息进行urldecode解析
                                        if (cps.indexOf("|") > 0) {
                                            String[] cpsArray = StringUtils.split(cps, "|");
                                            if (null != cpsArray && cpsArray.length >= 4) {
                                                String mid = cpsArray[0];
                                                if (StringUtils.isNotBlank(mid)) {
                                                    clientMap.put(ClientInfoType.MID.getClientInfoType(), mid);
                                                }
                                                String cid = cpsArray[1];
                                                if (StringUtils.isNotBlank(cid)) {
                                                    clientMap.put(ClientInfoType.CID.getClientInfoType(), cid);
                                                }
                                                String pid = cpsArray[2];
                                                if (StringUtils.isNotBlank(pid)) {
                                                    clientMap.put(ClientInfoType.PID.getClientInfoType(), pid);
                                                }
                                                String lpid = cpsArray[3];
                                                if (StringUtils.isNotBlank(lpid)) {
                                                    clientMap.put(ClientInfoType.LPID.getClientInfoType(), lpid);
                                                }

                                                // 对laifeng渠道的特殊处理
                                                if (clientMap.containsKey("pid")
                                                        && "laifeng".equals(clientMap.get("pid"))) {
                                                    // 3550325730_57%7C272%7C82001%7C0___
                                                    // 3550325730_57%7C272%7C82001%7C0___
                                                    clientMap.put(ClientInfoType.MID.getClientInfoType(), "57");
                                                    clientMap.put(ClientInfoType.CID.getClientInfoType(), "272");
                                                    clientMap.put(ClientInfoType.PID.getClientInfoType(), "82001");
                                                    clientMap.put(ClientInfoType.LPID.getClientInfoType(), "0");
                                                }

                                            }
                                        } else if (cps.indexOf("%7C") > 0) {
                                            try {
                                                String cpsStr = URLDecoder.decode(cps, "utf-8");
                                                if (StringUtils.isNotBlank(cpsStr) && cpsStr.indexOf("|") > 0) {
                                                    String[] cpsArray = StringUtils.split(cpsStr, "|");
                                                    if (null != cpsArray && cpsArray.length >= 4) {
                                                        String mid = cpsArray[0];
                                                        if (StringUtils.isNotBlank(mid)) {
                                                            clientMap.put(ClientInfoType.MID.getClientInfoType(), mid);
                                                        }
                                                        String cid = cpsArray[1];
                                                        if (StringUtils.isNotBlank(cid)) {
                                                            clientMap.put(ClientInfoType.CID.getClientInfoType(), cid);
                                                        }
                                                        String pid = cpsArray[2];
                                                        if (StringUtils.isNotBlank(pid)) {
                                                            clientMap.put(ClientInfoType.PID.getClientInfoType(), pid);
                                                        }
                                                        String lpid = cpsArray[3];
                                                        if (StringUtils.isNotBlank(lpid)) {
                                                            clientMap
                                                                    .put(ClientInfoType.LPID.getClientInfoType(), lpid);
                                                        }
                                                    }
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                        }

                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return clientMap;

    }

    public static String getStringValue(String param) {
        if (StringUtils.isNotBlank(param)) {
            return param;
        } else {
            return StringUtils.EMPTY;
        }
    }

    public static String getStringResult(String param) {
        if (StringUtils.isNumeric(param)) {
            return param;
        } else {
            return StringUtils.EMPTY;
        }
    }

    public static int getIntValue(String param) {
        if (StringUtils.isNumeric(param)) {
            return Integer.parseInt(param);
        } else {
            return 0;
        }
    }

    public static Long getLongValue(String param) {
        if (StringUtils.isNotBlank(param)) {
            return Long.parseLong(param);
        } else {
            return Long.parseLong(StringUtils.EMPTY);
        }
    }

    public static String getClientMapValue(Map<String, String> clientMap, String key) {
        String value = StringUtils.EMPTY;
        if (null!=clientMap && clientMap.size() > 0 && clientMap.containsKey(key) && StringUtils.isNotBlank(clientMap.get(key))) {
            value = clientMap.get(key);
        }
        return value;
    }

    public static Boolean ContentFilter(String content) {
        Boolean flag = Boolean.TRUE;
        if (content.startsWith("[") && content.endsWith("]")) {
            flag = Boolean.FALSE;
        }
        if (content.startsWith("哈") && content.endsWith("哈")) {
            flag = Boolean.FALSE;
        }
        if (content.startsWith(".") && content.endsWith(".")) {
            flag = Boolean.FALSE;
        }
        if (content.startsWith("...") && content.endsWith("...")) {
            flag = Boolean.FALSE;
        }
        if (content.startsWith("。") && content.endsWith("。")) {
            flag = Boolean.FALSE;
        }
        if (content.startsWith("、") && content.endsWith("、")) {
            flag = Boolean.FALSE;
        }
        if (content.startsWith("1") && content.endsWith("1")) {
            flag = Boolean.FALSE;
        }
        return flag;
    }

    /**
     * 判断消费类型
     * @param type
     * @return
     */
    public static boolean ConsumeTypeFilter(String type) {
        Boolean flag = Boolean.FALSE;
        if(StringUtils.isNotBlank(type) && StringUtils.isNotBlank(type)){
            int typeValue = Integer.parseInt(type);
            if(CONSUME_SET.contains(typeValue)){
                flag = Boolean.TRUE;
            }
        }
        return flag;
    }

    
    /**
     * 是否是纯数字
     * @param str
     * @return true为纯数字  false为非纯数字
     */
    public static boolean isAllNumber(String str){
    	String reg = "^\\d+$";
    	return str.trim().matches(reg);
    }
    
    
    public static void main(String args[]) {

        /*String clientInfo = "{\"clientInfo\":{\"osVersion\":\"android_4.2.2\",\"appId\":\"1001\",\"appVersion\":\"4\",\"channelId\":\"0_0|0|laifeng|0\",\"deviceToken\":\"2fc9e88eb5dbb2c8fa3d3f025521d6be\",\"userId\":\"-1\"}}";
        String clientInfoA = "{\"clientInfo\":{\"osVersion\":\"android_4.2.2\",\"appId\":\"1001\",\"appVersion\":\"4\",\"channelId\":\"3550325730_57%7C239%7C61729%7C0___\",\"deviceToken\":\"2fc9e88eb5dbb2c8fa3d3f025521d6be\",\"userId\":\"-1\"}}";

        Map<String, String> map = getClientMap(clientInfo);
        System.out.println(map.get("mid"));
        System.out.println(map.get("cid"));
        System.out.println(map.get("pid"));
        System.out.println(map.get("lpid"));
        Map<String, String> mapA = getClientMap(clientInfoA);
        System.out.println(mapA.get("mid"));
        System.out.println(mapA.get("cid"));
        System.out.println(mapA.get("pid"));
        System.out.println(mapA.get("lpid"));

        String dataInfo = "{\"dataInfo\":[{\"category\":\"plugin.system\",\"room\":\"98154987\",\"cpu\":\"Architecture:x86, Intel(R) Core(TM) i5-3470 CPU @ 3.20GHz, Number of cpu:4\",\"memory\":\"Physical Memory:3496MB, Available Physical Memory:777MB\",\"DNS\":\"DNS:10.155.100.20 10.155.100.21\",\"logtime\":\"1418699662510\",\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0\"}]}";
        JSONObject one = getClientDataJson(dataInfo);
        System.out.println(one.toString());*/
        /*String content = "[爱你][爱你][爱你][爱你][爱你][爱你]";
        System.out.println(ContentFilter(content));
        content = "哈哈哈哈";
        System.out.println(ContentFilter(content));
        content = "。。。。。。";
        System.out.println(ContentFilter(content));
        content = "、、、、、、";
        System.out.println(ContentFilter(content));
        content = ".......";
        System.out.println(ContentFilter(content));
*/
    	String str = "01";
    	System.out.println(isAllNumber(str));
    }

    /**
     * @return
     */
    public static String getOs(String osVersionStr) {
        String os = StringUtils.EMPTY;
        if (StringUtils.isNotBlank(osVersionStr)) {
            String[] arrayOs = StringUtils.split(osVersionStr, "_");
            if (null != arrayOs && arrayOs.length >= 2 && StringUtils.isNotBlank(arrayOs[0])) {
                os = arrayOs[0];
            }
        }
        return os;
    }

    /**
     * @return
     */
    public static String getOsVersion(String osVersionStr) {
        String osVersion = StringUtils.EMPTY;
        if (StringUtils.isNotBlank(osVersionStr)) {
            String[] arrayOs = StringUtils.split(osVersionStr, "_");
            if (null != arrayOs && arrayOs.length >= 2 && StringUtils.isNotBlank(arrayOs[1])) {
                osVersion = arrayOs[1];
            }
        }
        return osVersion;
    }

    /**
     * 获取第一条记录
     * 
     * @return
     */
    public static JSONObject getClientDataJson(String dataInfo) {
        JSONObject dataJson = null;
        if (StringUtils.isNotBlank(dataInfo) && !"{}".equals(dataInfo)) {
            dataJson = new JSONObject();
            JSONObject dataInfoJson = JSONObject.parseObject(dataInfo);
            if (null != dataInfoJson && dataInfoJson.containsKey("dataInfo")) {
                JSONArray arrayJson = new JSONArray();
                arrayJson = dataInfoJson.getJSONArray("dataInfo");
                if (null != arrayJson && arrayJson.size() > 0 && null != arrayJson.get(0)) {
                    // 获取第一条记录
                    dataJson = (JSONObject) arrayJson.get(0);
                }
            }
        }
        return dataJson;
    }

    /**
     * @param sendSpeed
     * @return
     */
    public static float getFloat(String sendSpeed) {
        float a = 0;
        if (StringUtils.isNotBlank(sendSpeed)) {
            float b = Float.parseFloat(sendSpeed);
            // 保留2位小数
            a = (float) (Math.round(b * 100)) / 100;
        }
        return a;
    }

    /**
     * 通过squenceid获取ip
     * @param squenceId
     * @return
     */
    public static String getIpBySeuenceId(String squenceId) {
        String ip = StringUtils.EMPTY;
        if(StringUtils.isNotBlank(squenceId)){
            List<String> list = Lists.newArrayList(Splitter.on("_").split(squenceId));
            if(null!=list && list.size() > 0 && StringUtils.isNotBlank(list.get(1))){
                ip = list.get(1);
            }
        }

        return ip;
    }


    /**
     * 通过squenceid获取ip
     * @param squenceId
     * @return
     */
    public static String getTokenBySeuenceId(String squenceId) {
        String Token = StringUtils.EMPTY;
        if(StringUtils.isNotBlank(squenceId)){
            List<String> list = Lists.newArrayList(Splitter.on("_").split(squenceId));
            if(null!=list && list.size() > 3 && StringUtils.isNotBlank(list.get(3))){
                Token = list.get(3);
            }
        }

        return Token;
    }
}
