package com.youku.ddshow.antispam.model;

/**
 * Created by dongjian on 2016/6/30.
 */
public class UgcUserPraiseRecordLog extends  UgcLog {
    private  Integer count;
    private  Integer anchorId;
    private  Integer roomId;
    private  Integer userId;
    private  String createTime;
    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getAnchorId() {
        return anchorId;
    }

    public void setAnchorId(Integer anchorId) {
        this.anchorId = anchorId;
    }

    public Integer getRoomId() {
        return roomId;
    }

    public void setRoomId(Integer roomId) {
        this.roomId = roomId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }


}
