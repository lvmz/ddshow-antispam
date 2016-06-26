package com.youku.ddshow.antispam.utils;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.youku.ddshow.antispam.model.PropertiesType;

public final class Database  implements Serializable {
	private DataSource ds = null;
	private Connection connection = null;
	private PropertiesType properties = null;
	private PreparedStatement prepstmt = null;
	private boolean error = false;

	private static final Logger LOG = LoggerFactory.getLogger(Database.class);
	
	public Database(PropertiesType type){
		properties = type;
	}
	/*
	 * get connection and return a Connection object
	 */
	private Connection getConnection() throws SQLException {
		try {
			InputStream in = Database.class.getClassLoader().getResourceAsStream(
					properties.getValue());
			System.out.println(properties.getValue());
			Properties props = new Properties();
			props.load(in);
			ds = DruidDataSourceFactory.createDataSource(props);
		} catch (Exception e) {
			LOG.error("getConnection error:" + e.getMessage());
		}
		return ds.getConnection();
	}

	public void execute(final String sql){
	    if(this.connection == null){
	        try {
	            connection = this.getConnection();
	        } catch (SQLException e) {
	            LOG.error("mysql execute:" + e.getMessage());
	        }
	    }

		if (this.connection == null) {
		    LOG.error("mysql execute:conn get failed. drop sql:" + sql);
			return;
		}
		
	    try {
	    	prepstmt = this.connection.prepareStatement(sql);
			prepstmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOG.error("execute:execute failed. message = " + e.getMessage()+" drop sql:" + sql);
			this.error = true;
		}
	    if(this.error){
	    	try {
	    		this.connection.close();
	    		this.connection = null;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				LOG.error("execute:conn.close failed. message = " + e.getMessage());
			}
	    }
	    this.error = false;
	}
	
	 /**
     * 执行sql获取结果
     * @param sql
     * @return
     */
    public ResultSet executeSql4Result(final String sql){
        ResultSet rs = null;
        if(StringUtils.isNotBlank(sql)){
            try {
                if(connection == null) {
                     connection = this.getConnection();
					System.out.println();
                }
               
            } catch (SQLException e) {
                e.printStackTrace();

                try {
                    connection.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }

            if (null == connection) {
                try {
                    connection.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }

            }
            try {
                PreparedStatement prepstmt = connection.prepareStatement(sql);
                rs = prepstmt.executeQuery(sql);

            } catch (SQLException e) {
                // TODO Auto-generated catch block
                LOG.error("execute:execute failed. message = " + e.getMessage()+" drop sql:" + sql);
                this.error = true;
            }
            if(this.error){
                try {
                    connection.close();
                    connection = null;
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    LOG.error("execute:conn.close failed. message = " + e.getMessage());
                }
            }
            this.error = false;
        }
        return rs;
    }
}
