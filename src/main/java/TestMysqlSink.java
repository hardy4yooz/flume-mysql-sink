/**
 * Created by Hardy on 2017/8/18.
 */

import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.flume.mysql.sink.MysqlSink;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.fail;

//import org.flume.mysql.sink.DAOClass;

public class TestMysqlSink {
    private Context context;
    private static final String testMessage = "Test message 1";
    private MysqlSink mySqlSink;
//    private static final String url = "jdbc:mysql://localhost:3306/employees";
//    private static final String user = "root";
//    private static final String password = "root";
//    private static final String driver = "com.mysql.jdbc.Driver";
//    private static final String tableName = "yzs_h5_accesslog";

    private static final String hostname = "localhost";
    private static final String port = "3306";
    private static final String databaseName = "reports";
    private static final String tableName = "yzs_h5_accesslog";
    private static final String user = "root";
    private static final String password = "root";
    private static final String columns = "VisitTime,FirstTime,WTID,IsFirstVisit,IsLogin,UserID,LoginPage,ReturnPage,LoginTime,SSUID,IP,URL_Host,URL,Ref_Host,Refer,SessionID,Language,Color,FlashVersion,Title,OS,Browser,Status,Screen,Source,SEdomain,Keyword,URL_Group,Ref_Group,Site,WTID_JRJ,WTID_SS,HomeModule";
//    private static final String values = "2017-04-21 10:47:30`2017-04-20 15:24:13`10.66.42.51-1492673053.329569`Return`NoLogin`-`-`-`-`-`10.66.42.51`itougu.jrj.com.cn`http://itougu.jrj.com.cn/match/v7/h5/index.jspa`-`-`2e70cbf5480bacf044d1492667130835.1492740304733`zh-CN`32`-`炒股大赛`Android6.0`Chrome/55.0`200`412x732`DirectTraffic`-`-`-`-`JRJ`-`-`-";
    private static final String values = "myevent:2017-08-18 16:30:10`2017-08-17 16:08:51`10.66.42.64-1502957331.341758`Return`NoLogin`-`-`-`-`-`10.66.42.64`itougu.jrj.com.cn`http://itougu.jrj.com.cn/match/v7/share/prepareGrabRed.jspa?redKey=8A98DC6B8796C5EECC27E29B960008103242451B24D71459&from=groupmessage`-`-`288979ffa2f715c05171502957330547.1503045009370`zh-CN`32`-`来金融界炒股大赛领红包`Android6.0`Chrome/53.0`200`360x640`DirectTraffic`-`-`-`-`JRJ`-`-`-`{\"WEBTRENDS_ID\":\"10.66.42.64-1502957331.341758\",\"Hm_lpvt_1f5bcd4b14ec462eb53cec277da30c3f\":\"1502957331\",\"redPhoneNo\":\"15201329975\",\"itgRedOpenId\":\"opmFds0ImJastnlfTR77QHnkfs50\",\"vjlast\":\"1502957331.1502957331.30\",\"Hm_lvt_1f5bcd4b14ec462eb53cec277da30c3f\":\"1502957331\",\"vjuids\":\"9c43bfee1.15def3ce12e.0.c2aabd4177211\"}\n";
    @Before
    public void setUp() {
        context = new Context();
        context.put("hostname", hostname);
        context.put("port", port);
        context.put("databaseName", databaseName);
        context.put("tableName", tableName);
        context.put("user", user);
        context.put("password", password);
        context.put("columns", columns);
        mySqlSink = new MysqlSink();
        mySqlSink.setName("Mysql sink");
        mySqlSink.configure(context);
    }

    @Test
    public void testMessageAdded() {
        Channel memoryChannel = new MemoryChannel();
        Configurables.configure(memoryChannel, context);
        mySqlSink.setChannel(memoryChannel);
        mySqlSink.start();
        Transaction txn = memoryChannel.getTransaction();
        txn.begin();
        Event event = EventBuilder.withBody(values.getBytes());
        memoryChannel.put(event);
        txn.commit();
        txn.close();
        try {
            Sink.Status status = mySqlSink.process();
            if (status == Sink.Status.BACKOFF) {
                fail("Error occured");
            }
        } catch (EventDeliveryException eDelExcp) {
            // noop
        }

//        getLatestMessage();

    }

//    public String getLatestMessage() {
//        DAOClass daoClass = new DAOClass();
//        String value = null;
//        try {
//            daoClass.createConnection(driver, url, user, password);
//            java.sql.Connection connection = daoClass.getConnection();
//            String sqlString = "SELECT message FROM MESSAGES ORDER BY created_at desc limit 1";
//            java.sql.Statement st = connection.createStatement();
//            ResultSet rs = st.executeQuery(sqlString);
//
//            while (rs.next()) {
//                value = rs.getString(1);
//            }
//
//            assertEquals("Values inserted not matching ", testMessage, value);
//            daoClass.destroyConnection(url, user);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//        return value;
//    }


}