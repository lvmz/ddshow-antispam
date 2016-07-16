package com.youku.ddshow.antispam.model;

public class UserRoomOnlineStatInfo {

	//日志版本(默认0)
    private int version;
    //时间戳
    private long timestamp;
    //访问IP
    private String ip;
    //应用ID
    private int appId;
    //日志类型:ugc.onlineTime
    private String category;
    //媒体Id
    private int cpsMid;
    //频道Id
    private int cpsCid;
    //推广位Id
    private int cpsPid;
    //落地页Id
    private int cpsLpid;
    //机器Id
    private String machineId;
    //优酷土豆Id
    private int ytId;
    //用户ID(0：匿名,其他：yId优酷Id)
    private int userId;
    //土豆ID
    private int tId;
    //用户登录:0代表游客,1代表登录用户
    private int userType;
    //vip类型
    private boolean isVip;
    //房间ID
    private int room;
    //主播ID
    private int anchorId;
    //应用版本号(1:1.0.0,2:1.0.1)
    private String appVersion;
    //操作系统版本号(ios:6.0.1,android:4.4.2)
    private String osVersion;
    //手机唯一标示号
    private String deviceToken;
    //该记录发起操作用户ID(0：匿名,其他：yId优酷Id)
    private int fk_origin_user;
    //该记录目标用户ID(0：匿名,其他：yId优酷Id)
    private int fk_target_user;
    //用户操作日志类型细分,默认为相关业务表
    private String ugcType;
    //相关数据Json
    private String dataInfo;
    //全局日志记录唯一ID
    private int sequenceId;
    //roomId
    private int roomId;
    //采样时间
    private long samplingTime;
    //登陆类型
    private int loginType;
    //人气值
    private int popularNum;
    
    //房间类型
    private String pattern;
    
	public String getPattern() {
		return pattern;
	}
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}
	public int getPopularNum() {
		return popularNum;
	}
	public void setPopularNum(int popularNum) {
		this.popularNum = popularNum;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getAppId() {
		return appId;
	}
	public void setAppId(int appId) {
		this.appId = appId;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public int getCpsMid() {
		return cpsMid;
	}
	public void setCpsMid(int cpsMid) {
		this.cpsMid = cpsMid;
	}
	public int getCpsCid() {
		return cpsCid;
	}
	public void setCpsCid(int cpsCid) {
		this.cpsCid = cpsCid;
	}
	public int getCpsPid() {
		return cpsPid;
	}
	public void setCpsPid(int cpsPid) {
		this.cpsPid = cpsPid;
	}
	public int getCpsLpid() {
		return cpsLpid;
	}
	public void setCpsLpid(int cpsLpid) {
		this.cpsLpid = cpsLpid;
	}
	public String getMachineId() {
		return machineId;
	}
	public void setMachineId(String machineId) {
		this.machineId = machineId;
	}
	public int getYtId() {
		return ytId;
	}
	public void setYtId(int ytId) {
		this.ytId = ytId;
	}
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public int getTId() {
		return tId;
	}
	public void setTId(int tId) {
		this.tId = tId;
	}
	public int getUserType() {
		return userType;
	}
	public void setUserType(int userType) {
		this.userType = userType;
	}
	public boolean isVip() {
		return isVip;
	}
	public void setVip(boolean isVip) {
		this.isVip = isVip;
	}
	public int getRoom() {
		return room;
	}
	public void setRoom(int room) {
		this.room = room;
	}
	public int getAnchorId() {
		return anchorId;
	}
	public void setAnchorId(int anchorId) {
		this.anchorId = anchorId;
	}
	public String getAppVersion() {
		return appVersion;
	}
	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}
	public String getOsVersion() {
		return osVersion;
	}
	public void setOsVersion(String osVersion) {
		this.osVersion = osVersion;
	}
	public String getDeviceToken() {
		return deviceToken;
	}
	public void setDeviceToken(String deviceToken) {
		this.deviceToken = deviceToken;
	}
	public int getFk_origin_user() {
		return fk_origin_user;
	}
	public void setFk_origin_user(int fk_origin_user) {
		this.fk_origin_user = fk_origin_user;
	}
	public int getFk_target_user() {
		return fk_target_user;
	}
	public void setFk_target_user(int fk_target_user) {
		this.fk_target_user = fk_target_user;
	}
	public String getUgcType() {
		return ugcType;
	}
	public void setUgcType(String ugcType) {
		this.ugcType = ugcType;
	}
	public String getDataInfo() {
		return dataInfo;
	}
	public void setDataInfo(String dataInfo) {
		this.dataInfo = dataInfo;
	}
	public int getSequenceId() {
		return sequenceId;
	}
	public void setSequenceId(int sequenceId) {
		this.sequenceId = sequenceId;
	}
	public int getRoomId() {
		return roomId;
	}
	public void setRoomId(int roomId) {
		this.roomId = roomId;
	}
	public int getLoginType() {
		return loginType;
	}
	public void setLoginType(int loginType) {
		this.loginType = loginType;
	}
	public long getSamplingTime() {
		return samplingTime;
	}
	public void setSamplingTime(long samplingTime) {
		this.samplingTime = samplingTime;
	}
    
   
}