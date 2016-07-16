package com.youku.ddshow.antispam.model;
public class UgcConnectLog extends UgcLog {
    private int roomId;

    public Long getSamplingTime() {
        return samplingTime;
    }

    public void setSamplingTime(Long samplingTime) {
        this.samplingTime = samplingTime;
    }

    private Long samplingTime;

    private int userId;

    private int mid;

    private int cid;

    private int pid;

    private int lpid;

    private int dt;

    private String version;

    private int loginType;

    private int flag;

    public void setRoomId(int roomId){
        this.roomId = roomId;
    }
    public int getRoomId(){
        return this.roomId;
    }

    public void setUserId(int userId){
        this.userId = userId;
    }
    public int getUserId(){
        return this.userId;
    }
    public void setMid(int mid){
        this.mid = mid;
    }
    public int getMid(){
        return this.mid;
    }
    public void setCid(int cid){
        this.cid = cid;
    }
    public int getCid(){
        return this.cid;
    }
    public void setPid(int pid){
        this.pid = pid;
    }
    public int getPid(){
        return this.pid;
    }
    public void setLpid(int lpid){
        this.lpid = lpid;
    }
    public int getLpid(){
        return this.lpid;
    }
    public void setDt(int dt){
        this.dt = dt;
    }
    public int getDt(){
        return this.dt;
    }
    public void setVersion(String version){
        this.version = version;
    }
    public String getVersion(){
        return this.version;
    }
    public void setLoginType(int loginType){
        this.loginType = loginType;
    }
    public int getLoginType(){
        return this.loginType;
    }
    public void setFlag(int flag){
        this.flag = flag;
    }
    public int getFlag(){
        return this.flag;
    }

}