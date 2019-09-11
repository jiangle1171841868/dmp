package com.itheima.kudu.spark;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @program: dmp
 * @author: Mr.Jiang
 * @create: 2019-09-10 19:06
 * @description: 创建KuduClient客户端连接
 **/
public class KuduClientTest {

    KuduClient kuduClient = null;

    /**
     * 初始化方法 获取连接
     */
    @Before
    public void init() {

        //todo 1.创建连接 构造者模式创建 (源码中有build 构造者模式就使用这种方式创建)
        String masterAddresses = "node01:7051,node02:7051,node03:7051";
        //参数:String masterAddresses   master的主机和端口信息   host:port
        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
                //设置超时时间
                .defaultSocketReadTimeoutMs(10000)
                .build();
    }

    /**
     * 创建表
     * create table itcast_user(
     * id int,
     * name string,
     * age byte,
     * primary key(id)
     *  )
     *  paritition by hash(id) partitions 3
     *  stored as kudu ;
     *
     * @throws KuduException
     */
    @Test
    public void createKuduTable() throws KuduException {

        String tableName = "itcast_users";
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();

        //todo 1.获取连接

        //todo 2.定义表的schema (列名 类型 是否为主键) schema = List(columnSchema)

        //a.构建ColumnSchema 列信息
        ColumnSchema id = new ColumnSchema
                //列名 类型
                .ColumnSchemaBuilder("id", Type.INT32)
                //是否是主键 可以多个主键 但是主键
                .key(true)
                .build();

        ColumnSchema name = new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).key(false).build();

        ColumnSchema age = new ColumnSchema.ColumnSchemaBuilder("age", Type.INT8).key(false).build();

        //b.将列添加到集合中
        columnSchemas.add(id);
        columnSchemas.add(name);
        columnSchemas.add(age);

        //c.构建Schema
        //参数:public Schema(List<ColumnSchema> columns)
        Schema schema = new Schema(columnSchemas);

        //todo 3.定义表的CreateTableOptions（表的属性）  设置分区 副本信息
        CreateTableOptions tableOptions = new CreateTableOptions();
        //a.hash分区
        /**
         * List<String> columns //分区的字段  封装在集合里面
         * int buckets          //几个分区
         */
        List<String> columns = new ArrayList<String>();

        //添加字段
        columns.add("id");

        int buckets = 3;
        tableOptions.addHashPartitions(columns, buckets);

        //b.设置副本
        tableOptions.setNumReplicas(3);


        //todo 4.创建表
        /**
         * public KuduTable createTable(
         *     String name, //表名
         *     Schema schema,//  schema对象  里面参数:List<ColumnSchema>
         *     CreateTableOptions builder// 表的属性  (设置分区 副本)
         *     )
         */
        KuduTable userTable = kuduClient.createTable(tableName, schema, tableOptions);

        //打印表的id
        System.out.println("userTable = " + userTable.getTableId());

        //todo 5.释放资源
    }

    /**
     * 删除表
     *
     * @throws KuduException
     */
    @Test
    public void dropKuduTable() throws KuduException {

        if (kuduClient.tableExists("itcast_users")) {
            DeleteTableResponse response = kuduClient.deleteTable("itcast_users");
            //获取删除 时间
            System.out.println("response = " + response.getElapsedMillis());

        }
    }


    /**
     * 将数据插入到Kudu Table中： INSERT INTO (id, name, age) VALUES (1001,zhangsan", 26)
     */
    @Test
    public void insertKuduTable() throws KuduException {

        String tableName = "itcast_users";

        //todo 1.获取连接

        //todo 2.根据表名 获取KuduTable的实例对象  叫做操作句柄
        KuduTable kuduTable = kuduClient.openTable(tableName);

        //todo 3.使用kuduClient  获取KuduSession实例对象 进行  增删改
        KuduSession kuduSession = kuduClient.newSession();

        //todo 4.使用kuduTable 构建insert实例对象 使用PartialRow封装数据  类似于hbase的put 一条数据 需要一个insert实例

        //设置手动刷新到表中
        kuduSession.setFlushMode(AsyncKuduSession.FlushMode.MANUAL_FLUSH);

        //设置缓存大小  数据条数
        kuduSession.setFlushInterval(20000);


        for (int i = 0; i < 100; i++) {
            //a.构建insert实例对象
            Insert insert = kuduTable.newInsert();
            //b.通过insert 获取PartialRow 根据类型封装数据
            PartialRow row = insert.getRow();
            row.addInt("id", 1000 + i);
            row.addString("name", "zhangsan" + i);
            //age是byte类型 数字默认是 int类型  需要强转成byte类型
            row.addByte("age", (byte) (20 + new Random().nextInt(10)));

            //c.插入数据到表中
            kuduSession.apply(insert);
        }

        //todo 5.刷新数据到表中
        kuduSession.flush();

        //todo 6.关闭回话
        kuduSession.close();
    }

    /**
     * 从kudu中查询数据
     *
     * @throws KuduException
     */
    @Test
    public void selectKuduTable() throws KuduException {

        String tableName = "itcast_users";
        //todo 1.获取连接

        //todo 2.根据表名 获取KuduTable的实例对象  叫做操作句柄
        KuduTable kuduTable = kuduClient.openTable(tableName);

        //todo 3.构建KuduScanner扫描器，进行查询数据  类似hbase的scan
        KuduScanner kuduScanner = kuduClient.newScannerBuilder(kuduTable).build();

        //todo 4.获取结果集
        /**
         * 并行查询  每个分区的结果集封装成一个迭代器 最终结果 将分区的迭代器封装成一个迭代器
         * 所以有两层迭代器
         */

        //定义一个变量 看数据是哪个分区
        int batch = 0;
        while (kuduScanner.hasMoreRows()) {

            //先输出 再自增
            System.out.println("batch = " + batch++);
            //a.获取分区数据
            RowResultIterator rowResults = kuduScanner.nextRows();

            //b.迭代获取分区数据
            while (rowResults.hasNext()) {

                //c.获取一条数据
                RowResult rowResult = rowResults.next();

                //c.根据类型获取每一列数据
                System.out.println(
                        "id=" + rowResult.getInt("id") +
                                ", name = " + rowResult.getString("name") +
                                ", age = " + rowResult.getByte("age")

                );
            }
        }

    }

    /**
     * 添加过滤条件查询  只查询id和age两个字段的值，年龄age小于25，id大于1050
     *
     * @throws KuduException
     */
    @Test
    public void selectKuduTableFilter() throws KuduException {

        String tableName = "itcast_users";
        //todo 1.获取连接

        //todo 2.根据表名 获取KuduTable的实例对象  叫做操作句柄
        KuduTable kuduTable = kuduClient.openTable(tableName);

        //todo 3.通过kuduClient构建KuduScanner扫描器，进行过滤数据

        //a.添加需要查询的字段到集合中
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("id");
        columnNames.add("name");
        columnNames.add("age");

        //b.设置条件 age小于25
        /**
         * 过滤条件是三要素
         * 1.比较的字段     // ColumnSchema column
         * 2.比较运算符     //ComparisonOp op
         * 3.比较的值      //long value
         */
        ColumnSchema age = new ColumnSchema.ColumnSchemaBuilder("age", Type.INT8).build();
        KuduPredicate ageKuduPredicate = KuduPredicate.newComparisonPredicate(
                age,
                KuduPredicate.ComparisonOp.LESS,
                25
        );


        //c.设置条件 id大于1050
        ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build();
        KuduPredicate idKuduPredicate = KuduPredicate.newComparisonPredicate(
                id,
                KuduPredicate.ComparisonOp.GREATER,
                1050
        );

        //d.执行查询
        KuduScanner kuduScanner = kuduClient.newScannerBuilder(kuduTable)
                //添加条件
                .addPredicate(idKuduPredicate)
                .addPredicate(ageKuduPredicate)
                //设置查询的字段
                .setProjectedColumnNames(columnNames)
                .build();

        //todo 4.获取结果集
        while (kuduScanner.hasMoreRows()) {

            RowResultIterator rowResults = kuduScanner.nextRows();

            while (rowResults.hasNext()) {
                RowResult rowResult = rowResults.next();
                System.out.println(
                        "id=" + rowResult.getInt("id") +
                                ", name = " + rowResult.getString("name") +
                                ", age = " + rowResult.getByte("age")

                );
            }
        }
    }


    /**
     * 范围分区
     *
     * @throws KuduException
     */
    @Test
    public void createKuduTableRange() throws KuduException {

        String tableName = "itcast_users_range";
        //todo 1.获取连接

        //todo 2.构建表的schema
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build();
        ColumnSchema name = new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build();
        ColumnSchema age = new ColumnSchema.ColumnSchemaBuilder("age", Type.INT8).build();

        columnSchemas.add(id);
        columnSchemas.add(name);
        columnSchemas.add(age);
        Schema schema = new Schema(columnSchemas);

        //todo 3.构架CreateTableOptions实例 设置分区 副本
        //1.构架实例
        CreateTableOptions tableOptions = new CreateTableOptions();

        //2.设置范围分区 范围分区的字段必须是主键 或者主键的一部分
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        tableOptions.setRangePartitionColumns(columns);

        //3.设置分区范围
        /**
         * value < 100
         * 100 <= value < 500
         * 500 <= value
         */
        /**
         * 包头不包尾
         * PartialRow lower,  下限值
         * PartialRow upper    上限值
         *
         * public PartialRow(Schema schema)
         * 实例需要参数:scheam
         */
        //a.value < 100
        PartialRow upper100 = new PartialRow(schema);
        upper100.addInt("id", 100);

        //添加分区范围  没有上限值 或者下限值 就使用new PartialRow(schema)
        tableOptions.addRangePartition(new PartialRow(schema), upper100);

        //b.100 <= value < 500
        PartialRow low100 = new PartialRow(schema);
        low100.addInt("id", 100);
        PartialRow upper500 = new PartialRow(schema);
        upper500.addInt("id", 500);

        tableOptions.addRangePartition(low100, upper500);

        //c.500 <= value
        PartialRow low500 = new PartialRow(schema);
        low500.addInt("id", 500);
        tableOptions.addRangePartition(low500, new PartialRow(schema));

        //4.设置副本
        tableOptions.setNumReplicas(3);

        //todo 4.创建表
        //String name, Schema schema, CreateTableOptions builder
        KuduTable userTable = kuduClient.createTable(tableName, schema, tableOptions);

        System.out.println(userTable.getTableId());

    }

    /**
     * 关闭资源
     *
     * @throws KuduException
     */
    @After
    public void clean() throws KuduException {
        if (null != kuduClient) kuduClient.close();
    }
}
