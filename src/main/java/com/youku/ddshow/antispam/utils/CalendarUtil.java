package com.youku.ddshow.antispam.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

/**
 * User: yuxiangwu Date: 13-7-8 Time: 下午1:43
 */
public final class CalendarUtil {

    private CalendarUtil() {

    }

    // 一个月默认天数
    private static final int DAYS = 30;

    // 一周默认天数
    private static final int WEEK_DAYS = 6;

    // 秒与毫秒转换值
    private static final int DEFAULT_CHANGE_SECOND = 1000;
    
    //过期5分钟
    private static final int EXPIRED_TIME = 300000;

    public static String getWeekString() {
        String weekStr = "";
        Calendar cal = Calendar.getInstance();
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK) - 2;
        cal.add(Calendar.DATE, -dayOfWeek);
        weekStr = weekStr + dateFormat(cal.getTime(), "yyyy-MM-dd");
        cal.add(Calendar.DATE, WEEK_DAYS);
        weekStr = weekStr + "~~" + dateFormat(cal.getTime(), "yyyy-MM-dd");
        return weekStr;
    }

    public static String getToday() {
        String weekStr = "";
        Calendar cal = Calendar.getInstance();
        weekStr = dateFormat(cal.getTime(), "yyyy-MM-dd");
        return weekStr;
    }

    public static String getTomorrow() {
        String weekStr = "";
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 1);
        weekStr = dateFormat(cal.getTime(), "yyyy-MM-dd");
        return weekStr;
    }

    public static String getDayAfter(Date day) {
        String weekStr = "";
        Calendar cal = Calendar.getInstance();
        cal.setTime(day);
        cal.add(Calendar.DATE, 1);
        weekStr = dateFormat(cal.getTime(), "yyyy-MM-dd");
        return weekStr;
    }

    public static String getYesterday() {
        String dayStr = "";
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        dayStr = dateFormat(cal.getTime(), "yyyy-MM-dd");
        return dayStr;
    }

    public static String getOneMonthDayBefore() {
        String weekStr = "";
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -DAYS);
        weekStr = dateFormat(cal.getTime(), "yyyy-MM-dd");
        return weekStr;
    }

    public static String dateFormat(Date date, String format) {
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(date);
    }

    public static String dateFormat(Date date) {
        return dateFormat(date, "yyyy-MM-dd");
    }

    /**
     * 获得当前的时间戳，单位：秒
     * 
     * @return
     */
    public static int getTimeStamp() {
        return (int) (System.currentTimeMillis() / DEFAULT_CHANGE_SECOND);
    }

    /**
     * 通过毫秒数获取指定日期的年份
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:07:50
     * @param p_date
     * @return int
     */
    public static int getYearOfDate(long logTime) {
        Date p_date = new Date(logTime);
        Calendar c = Calendar.getInstance();
        c.setTime(p_date);
        return c.get(Calendar.YEAR);
    }

    /**
     * 获取指定日期的月份
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:08:26
     * @param p_date
     * @return int
     */
    public static int getMonthOfDate(long logTime) {
        Date p_date = new Date(logTime);
        Calendar c = Calendar.getInstance();
        c.setTime(p_date);
        return c.get(Calendar.MONTH) + 1;
    }

    /**
     * 获取指定日期的日份
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:08:56
     * @param p_date
     * @return int
     */
    public static int getDayOfDate(String logTime) {
        Date p_date = new Date(Long.valueOf(logTime));
        Calendar c = Calendar.getInstance();
        c.setTime(p_date);
        return c.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * 获取指定日期的小时
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:09:24
     * @param p_date
     * @return int
     */
    public static int getHourOfDate(long logTime) {
        Date p_date = new Date(logTime);
        Calendar c = Calendar.getInstance();
        c.setTime(p_date);
        return c.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 获取指定日期的分钟
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:09:40
     * @param p_date
     * @return int
     */
    public static int getMinuteOfDate(Date p_date) {
        Calendar c = Calendar.getInstance();
        c.setTime(p_date);
        return c.get(Calendar.MINUTE);
    }

    /**
     * 获取指定日期的秒钟
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:09:55
     * @param p_date
     * @return int
     */
    public static int getSecondOfDate(Date p_date) {
        Calendar c = Calendar.getInstance();
        c.setTime(p_date);
        return c.get(Calendar.SECOND);
    }

    /**
     * 获取指定日期的毫秒
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:10:12
     * @param p_date
     * @return long
     */
    public static long getMillisOfDate(Date p_date) {
        Calendar c = Calendar.getInstance();
        c.setTime(p_date);
        return c.getTimeInMillis();
    }

    /**
     * 获取完整日期
     * 
     * @author qiaowang
     * @date 2014-2-19 上午10:33:48
     * @param date
     * @return String
     */
    public static String getDateFormat(Long logTime) {
        Date date = new Date(logTime);
        return dateFormat(date, "yyyy-MM-dd");
    }

    /**
     * 获取时 分 秒
     * 
     * @author qiaowang
     * @date 2014-5-4 上午11:16:51
     * @param logTime
     * @return String
     */
    public static String getDetailDateFormat(Long logTime) {
        Date date = new Date(logTime);
        return dateFormat(date, "yyyy-MM-dd HH:mm:ss");
    }
    
    /**
     * 获取 过期时 分 秒
     * 
     * @author qiaowang
     * @date 2014-5-4 上午11:16:51
     * @param logTime
     * @return String
     */
    public static String getDetailExpireDateFormat(Long logTime) {
        Date date = new Date(logTime + EXPIRED_TIME);
        return dateFormat(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 获取时 分
     * 
     * @author qiaowang
     * @date 2014-5-4 上午11:16:51
     * @param logTime
     * @return String
     */
    public static String getDetailMinFormat(Long logTime) {
        Date date = new Date(logTime);
        return dateFormat(date, "yyyy-MM-dd HH:mm");
    }

    /**
     * 获取时 分 5分钟切分
     * 
     * @author qiaowang
     * @date 2014-5-4 上午11:16:51
     * @param logTime
     * @return String
     */
    public static String getDetailFiveMinFormat(Long logTime) {
        Date date = new Date(logTime);
        int min = getMinuteOfDate(date);
        StringBuilder buf = new StringBuilder();
        buf.append(dateFormat(date, "yyyy-MM-dd HH"));
        if(min == 0){
            buf.append(":00");
        } else if(min > 0 && min <= 5) {
            buf.append(":05");
        } else if (min > 5 && min <= 10) {
            buf.append(":10");
        } else if (min > 10 && min <= 15) {
            buf.append(":15");
        } else if (min > 15 && min <= 20) {
            buf.append(":20");
        } else if (min > 20 && min <= 25) {
            buf.append(":25");
        } else if (min > 25 && min <= 30) {
            buf.append(":30");
        } else if (min > 30 && min <= 35) {
            buf.append(":35");
        } else if (min > 35 && min <= 40) {
            buf.append(":40");
        } else if (min > 40 && min <= 45) {
            buf.append(":45");
        } else if (min > 45 && min <= 50) {
            buf.append(":50");
        } else if (min > 50 && min <= 55) {
            buf.append(":55");
        } else if (min > 55 && min < 60) {
            StringBuilder str = new StringBuilder();
            int hour = getHourOfDate(logTime) + 1;
            String hstr = StringUtils.EMPTY;
            if (hour >= 24) {
                hstr = "00";
                str.append(getDayAfter(date)).append(" ").append(hstr).append(":00");
                return str.toString();
            } else if (hour < 10) {
                hstr = "0" + hour;
            } else {
                hstr = "" + hour;
            }
            str.append(dateFormat(date, "yyyy-MM-dd")).append(" ").append(hstr).append(":00");
            return str.toString();
        }
        return buf.toString();
    }

    public static String getRelativeToDateBeforeday(String date, String pattern, int beforeDays) {
        String dayStr = "";
        Calendar cal = Calendar.getInstance();
        Date dd = dateStringParse(date, pattern);
        cal.setTime(dd);
        cal.add(Calendar.DATE, -beforeDays);
        dayStr = dateFormat(cal.getTime(), pattern);
        return dayStr;
    }

    public static Date dateStringParse(String dateString, String pattern) {
        if (StringUtils.isBlank(dateString)) {
            return new Date();
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);

        Date d = null;
        try {
            d = sdf.parse(dateString);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return d;
    }

    /**
     * 获取一天的结束时间
     *
     * @param date
     * @return
     */
    public static Calendar getDayEnd(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar;
    }
    
    public static void main(String[] args) {
        int year = getYearOfDate(Long.parseLong("1392348772297"));
        int month = getMonthOfDate(Long.parseLong("1392348772297"));
        int day = getDayOfDate("1392348772297");
        System.out.println("1392348772297");
        System.out.println(year);
        System.out.println(month);
        System.out.println(day);
        System.out.println(getDateFormat(Long.parseLong("1409068859189")));
        System.out.println(getDetailDateFormat(Long.parseLong("1392348772297")));

        String today = "2014-05-06";
        String tmpDay = getRelativeToDateBeforeday(today, "yyyy-MM-dd", 30);
        System.out.println(tmpDay);

        System.out.println(getMinuteOfDate(new Date(Long.parseLong("1409068859189"))));
        System.out.println(getDetailMinFormat(Long.parseLong("1408204699989")));
        System.out.println(getDetailFiveMinFormat(Long.parseLong("1408204699989")));
    }

}
