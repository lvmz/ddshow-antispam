package com.youku.ddshow.antispam.model;

import java.util.Date;

/**
 * Created by dongjian on 2016/7/4.
 */
public class UgcChat extends UgcLog {
    private int anchorLevel;

    private String content;

    private String createDate;

    private int id;

    private String methodName;

    private int originUserId;

    private String originUserName;

    private int roomId;

    private int screenId;

    private int single;

    private int status;

    private int targetUserId;

    private String targetUserName;

    private int userLevel;


    public String getCreateDate() {
        return createDate;
    }
    public void setCreateDate(String createDate) {
        this.createDate = createDate;
    }
    public void setAnchorLevel(int anchorLevel){
        this.anchorLevel = anchorLevel;
    }
    public int getAnchorLevel(){
        return this.anchorLevel;
    }
    public void setContent(String content){
        this.content = content;
    }
    public String getContent(){
        return this.content;
    }
    public void setId(int id){
        this.id = id;
    }
    public int getId(){
        return this.id;
    }
    public void setMethodName(String methodName){
        this.methodName = methodName;
    }
    public String getMethodName(){
        return this.methodName;
    }
    public void setOriginUserId(int originUserId){
        this.originUserId = originUserId;
    }
    public int getOriginUserId(){
        return this.originUserId;
    }
    public void setOriginUserName(String originUserName){
        this.originUserName = originUserName;
    }
    public String getOriginUserName(){
        return this.originUserName;
    }
    public void setRoomId(int roomId){
        this.roomId = roomId;
    }
    public int getRoomId(){
        return this.roomId;
    }
    public void setScreenId(int screenId){
        this.screenId = screenId;
    }
    public int getScreenId(){
        return this.screenId;
    }
    public void setSingle(int single){
        this.single = single;
    }
    public int getSingle(){
        return this.single;
    }
    public void setStatus(int status){
        this.status = status;
    }
    public int getStatus(){
        return this.status;
    }
    public void setTargetUserId(int targetUserId){
        this.targetUserId = targetUserId;
    }
    public int getTargetUserId(){
        return this.targetUserId;
    }
    public void setTargetUserName(String targetUserName){
        this.targetUserName = targetUserName;
    }
    public String getTargetUserName(){
        return this.targetUserName;
    }
    public void setUserLevel(int userLevel){
        this.userLevel = userLevel;
    }
    public int getUserLevel(){
        return this.userLevel;
    }

}
