/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flume.mysql.sink;

/**
 * @author ml.li
 */

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

public class MysqlSink extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(MysqlSink.class);
    private String hostname;
    private String port;
    private String databaseName;
    private String tableName;
    private String user;
    private String password;
    private String columns;
    private PreparedStatement preparedStatement;
    private Connection conn;
    private int batchSize;

    public MysqlSink() {
        LOG.info("MysqlSink start...");
    }

    public void configure(Context context) {
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set!!");
        databaseName = context.getString("databaseName");
        Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
        tableName = context.getString("tableName");
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        user = context.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");
        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        batchSize = context.getInteger("batchSize", 100);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
        columns = context.getString("columns");
        Preconditions.checkNotNull(columns, "columns must be set!!");
    }

    @Override
    public void start() {
        super.start();
        try {
            //调用Class.forName()方法加载驱动程序  
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName;
        //调用DriverManager对象的getConnection()方法，获得一个Connection对象  

        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            //创建一个Statement对象  
            preparedStatement = conn.prepareStatement(this.create_sql());

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    private String create_sql() {
        StringBuilder sb = new StringBuilder();
        StringBuilder column_sb = new StringBuilder();
        StringBuilder value_sb = new StringBuilder();
        int cnt = 1;
        String[] columns_arr = columns.split(",");
        for (String column : columns_arr) {
            column_sb.append(column);
            value_sb.append("?");
            if (cnt != columns_arr.length) {
                column_sb.append(",");
                value_sb.append(",");
            }
            cnt++;
        }
        sb.append("insert into ").append(tableName);
        sb.append(" ( ");
        sb.append(column_sb);
        sb.append(" ) ").append(" values").append("(");
        sb.append(value_sb);
        sb.append(" ) ");
        return sb.toString();
    }

    @Override
    public void stop() {
        super.stop();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;

        List<String> actions = Lists.newArrayList();
        transaction.begin();
        try {
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    content = new String(event.getBody());
                    actions.add(content);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            if (actions.size() > 0) {
                preparedStatement.clearBatch();
                for (String temp : actions) {
                    String[] values_arr = temp.split("`");
                    String[] columns_arr = columns.split(",");
                    if (values_arr.length >= columns_arr.length) {
                        int index = 1;
                        for (int i = 0; i < columns_arr.length; i++) {
                            preparedStatement.setString(index, values_arr[i]);
                            index++;
                        }
                        preparedStatement.addBatch();
                    }
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
            transaction.commit();
        } catch (Throwable e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            LOG.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            transaction.close();
        }

        return result;
    }
}  

