package com.q1.datax.plugin.writer.kudu11xwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author daizihao
 * @create 2020-08-31 16:55
 **/
public class KuduWriterTask {
    private final static Logger LOG = LoggerFactory.getLogger(KuduWriterTask.class);

    public List<Configuration> columns;
    public String encoding;
    public String insertMode;
    public Double batchSize;
    public long mutationBufferSpace;
    public boolean isUpsert;

    public KuduClient kuduClient;
    public KuduTable table;
    KuduSession session;


    public KuduWriterTask(com.alibaba.datax.common.util.Configuration configuration) {
        this.columns = configuration.getListConfiguration(Key.COLUMN);
        this.encoding = configuration.getString(Key.ENCODING);
        this.insertMode = configuration.getString(Key.INSERT_MODE);
        this.batchSize = configuration.getDouble(Key.WRITE_BATCH_SIZE);
        this.mutationBufferSpace = configuration.getLong(Key.MUTATION_BUFFER_SPACE);
        this.isUpsert = !configuration.getString(Key.INSERT_MODE).equals("insert");

        this.kuduClient = Kudu11xHelper.getKuduClient(configuration.getString(Key.KUDU_CONFIG));
        this.table = Kudu11xHelper.getKuduTable(configuration, kuduClient);
        this.session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace((int) mutationBufferSpace);
//        tableName = configuration.getString(Key.TABLE);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector) {
        Record record;
        AtomicLong counter = new AtomicLong(0L);
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != columns.size()) {
                    throw DataXException.asDataXException(Kudu11xWriterErrorcode.PARAMETER_NUM_ERROR, "读出字段个数:" + record.getColumnNumber() + " " + "配置字段个数:" + columns.size());
                }
                boolean isDirtyRecord = false;

                for (int i = 0; i < columns.size() && !isDirtyRecord; i++) {
                    Configuration col = columns.get(i);
                    if (!col.getBool(Key.PRIMARYKEY, false)) {
                        break;
                    }
                    Column column = record.getColumn(col.getInt(Key.INDEX,i));
                    isDirtyRecord = (StringUtils.isBlank(column.asString()));
                }
                if (isDirtyRecord) {
                    taskPluginCollector.collectDirtyRecord(record, "primarykey字段为空");
                    continue;
                }

                Upsert upsert = table.newUpsert();
                Insert insert = table.newInsert();

                for (int i = 0; i < columns.size(); i++) {
                    PartialRow row;
                    if (isUpsert) {
                        //覆盖更新
                        row = upsert.getRow();
                    } else {
                        //增量更新
                        row = insert.getRow();
                    }
                    Configuration col = columns.get(i);
                    String name = col.getString(Key.NAME);
                    ColumnType type = ColumnType.getByTypeName(col.getString(Key.TYPE));
                    Column column = record.getColumn(col.getInt(Key.INDEX,i));
                    Object rawData = column.getRawData();
                    if (rawData == null) {
                        row.setNull(name);
                        continue;
                    }
                    switch (type) {
                        case INT:
                        case LONG:
                        case BIGINT:
                            row.addLong(name, Long.parseLong(rawData.toString()));
                            break;
                        case FLOAT:
                            row.addFloat(name, Float.parseFloat(rawData.toString()));
                            break;
                        case STRING:
                            row.addString(name, rawData.toString());
                            break;
                        case DOUBLE:
                            row.addDouble(name, Double.parseDouble(rawData.toString()));
                            break;
                        case BOOLEAN:
                            row.addBoolean(name, Boolean.getBoolean(rawData.toString()));
                            break;
                    }
                }
                try {
                    if (isUpsert) {
                        //覆盖更新
                        session.apply(upsert);
                    } else {
                        //增量更新
                        session.apply(insert);
                    }
                    //提前写数据，阈值可自定义
                    if (counter.incrementAndGet() > batchSize * 0.75) {
                        session.flush();
                        counter.set(0L);
                    }
                } catch (KuduException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(Kudu11xWriterErrorcode.PUT_KUDU_ERROR, e);
        }
        AtomicInteger i = new AtomicInteger(10);
        try {
            while (i.get() > 0) {
                if (session.hasPendingOperations()) {
                    session.flush();
                    break;
                }
                Thread.sleep(1000L);
                i.decrementAndGet();
            }
        } catch (Exception e) {
            LOG.info("sesion刷写中" + i + "s .....");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            i.decrementAndGet();
        } finally {
            try {
                session.flush();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }

    }


}
