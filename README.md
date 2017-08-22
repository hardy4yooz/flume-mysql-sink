# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources = r1
a1.channels = c1
a1.sources.r1.type = TAILDIR
a1.sources.r1.channels = c1
a1.sources.r1.positionFile =/Users/caoxiaozhen/Documents/apache-flume-1.7.0-bin/positionJson/flume/flume2mysql_position.json 
a1.sources.r1.filegroups = f2
a1.sources.r1.filegroups.f2 =/Users/caoxiaozhen/Documents/logs/.*log.*
a1.sources.r1.headers.f2.headerKey1 = value2
a1.sources.r1.headers.f2.headerKey2 = value2-2
a1.sources.r1.fileHeader = true

# describe the interceptor

a1.sources.r1.interceptors = i1 i2
a1.sources.r1.interceptors.i1.type=com.jrj.dmc.flume.interceptor.JRJSDCInterceptor$Builder


a1.sources.r1.interceptors.i2.type = org.apache.flume.sink.solr.morphline.UUIDInterceptor$Builder
a1.sources.r1.interceptors.i2.headerName = key


# Describe the sink
a1.sinks.k1.type  = org.flume.mysql.sink.MysqlSink
a1.sinks.k1.hostname = localhost
a1.sinks.k1.port = 3306
a1.sinks.k1.databaseName = reports
a1.sinks.k1.tableName = yzs_h5_accesslog
a1.sinks.k1.user = root
a1.sinks.k1.password = root
a1.sinks.k1.channel = c1
a1.sinks.k1.columns = VisitTime,FirstTime,WTID,IsFirstVisit,IsLogin,UserID,LoginPage,ReturnPage,LoginTime,SSUID,IP,URL_Host,URL,Ref_Host,Refer,SessionID,Language,Color,FlashVersion,Title,OS,Browser,Status,Screen,Source,SEdomain,Keyword,URL_Group,Ref_Group,Site,WTID_JRJ,WTID_SS,HomeModule

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100


# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
