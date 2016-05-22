package com.youku.ddshow.antispam.utils;

import com.youku.ddshow.antispam.model.PropertiesType;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DruidTest {
	public static void main(String[] args) {
		Database db = new Database(PropertiesType.DDSHOW_STAT_TEST);
        try
		{
			db.execute(String.format("insert into t_result_ugc_antispam (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
					,"852908128", "175.21.56.34", "5e9ee685-2ea6-4a6d-996d-f2de6f166778",  "馥芳", "899350",
					"怎么不露脸啊",CalendarUtil.getDetailDateFormat(1462506026585L),"0"));


			System.out.println(String.format("insert into t_result_ugc_antispam (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
					,"852908128", "175.21.56.34", "5e9ee685-2ea6-4a6d-996d-f2de6f166778",  "馥芳", "899350",
					"怎么不露脸啊",CalendarUtil.getDetailDateFormat(1462506026585L),"0"));
		}catch (Exception e)
		{
			e.printStackTrace();
		}

	}
}
