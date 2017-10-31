#coding:utf8
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *

import os

if __name__ == '__main__':
    os.system('kinit -kt /etc/security/keytabs/xfyan.keytab xfyan/bgs-5p242-yanxufei@BFD.COM')

    socket = TSocket.TSocket('172.24.5.242', 9090)
    #transport = TTransport.TBufferedTransport(socket)
    transport = TTransport.TSaslClientTransport(socket, host='bgs-5p242-yanxufei', service='xfyan', mechanism='GSSAPI')
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    transport.open()

    #获取所有表名，返回list
    client.getTableNames()
    #使能表，返回true false
    client.enableTable(tableName='xfyan_test')
    #不使能表
    client.disableTable(tableName='xfyan_test')
    #判断表是否使能 返回true false
    client.isTableEnabled(tableName='xfyan_test')
    #获取列族信息，返回字典
    client.getColumnDescriptors(tableName='xfyan_test')
    #获取表下的所有region，返回list
    client.getTableRegions(tableName='xfyan_test')
    #建表
    columnFamily = ColumnDescriptor(name='test')
    client.createTable(tableName='xfyan_test',columnFamilies=[columnFamily,])
    #删除表（要先disable）
    client.deleteTable(tableName='xfyan_test')
    # 数据cell获取  返回list[TCell类]。提取数据用p[0].__dict__
    p = client.get(tableName='xfyan_test',row='rowkey',column='name',attributes=None)
    #检索条件多加一个timestamp
    client.getVerTs(tableName='xfyan_test', row='rowkey', column='name', timestamp=1446708556096, numVersions=2, attributes=None)
    # 获取一行所有信息，p[0]__dict__提取信息
    p = client.getRow(tableName='xfyan_test', row='rowkey', attributes=None)
    # 指定行列获取一行所有信息，p[0]__dict__提取信息.
    p = client.getRowWithColumns(tableName='xfyan_test', row='rowkey',columns=['name','age',] , attributes=None)
    # 指定行列获取一行所有信息，p[0]__dict__提取信息.
    # 返回示例   {'sorted':None,'columns':{'column':TCell(timestamp=12345667,value='1234')},'row':'2'}
    p = client.getRowTs(tableName='xfyan_test', row='rowkey', timestamp=1446708556096, attributes=None)
    # 指定行列获取一行所有信息，p[0]__dict__提取信息.
    # 返回示例   {'sorted':None,'columns':{'column':TCell(timestamp=12345667,value='1234')},'row':'2'}
    p = client.getRowWithColumnsTs(tableName='xfyan_test', row='rowkey', columns=['name','age',], timestamp=1446708556096, attributes = None)
    # 获取行数据  数据类型   TRowResult  p[0]__dict__提取信息.
    client.getRows(tableName='xfyan_test', rows=[1446708556096,1446708556096, ], attributes=None)
    # 获取行数据  数据类型TRowResult  p[0]__dict__提取信息. 匹配检索的列族
    p = client.getRowsWithColumns(tableName='xfyan_test',rows=[1446708556096,1446708556096, ], columns=['name','age',], attributes=None)
    # 获取行数据  数据类型TRowResult  p[0]__dict__提取信息. 匹配检索的列族,多个时间戮约束
    p = client.getRowsWithColumnsTs( tableName='xfyan_test', rows=[1446708556096,1446708556096, ], columns=['name','age',], timestamp=1446708556096, attributes = None)
    # 注意。只能是对某行的列族数据的更新或者删除操作，是创建列族的操作，如果新建行，可以做为创建列族详情见Mutation结构定义
    mu = [Mutation(isDelete=1, column='name', value="1233"), ]
    p = client.mutateRow('xfyan_test', row='1446708556096', mutations=mu, attributes=None)
    # 注意。只能是对某行的列族数据的更新或者删除操作，是创建列族的操作，如果新建行，可以做为创建列族详情见Mutation结构定义，
    # 多加一个时间戮，删除做为约束条件，添加数据这个参数无意义
    mu = [Mutation(isDelete=1, column='column:name', value="1233"), ]
    p = client.mutateRowTs('xfyan_test', row='1446708556096', mutations=mu, timestamp=24234, attributes=None)
    # 对列族中字段值相加，可以做为计数器使用
    client.atomicIncrement(tableName='xfyan', row='1446708556096', column='name', value="1233")
    # 删除对应的单元格数据值，不删除列族字段
    client.deleteAll(tableName='xfyan_test', row='1446708556096',column='name', attributes=None)
    # 删除对应的单元格数据值，不删除列族字段,多加一个时间戮约束,时间戳是等于或大于过去的时间戳。
    client.deleteAllTs(tableName='xfyan_test',  row='1446708556096',column='name', timestamp=24234, attributes=None)
    # 删除对应行所有数据
    client.deleteAllRow('tableName', 'row', attributes=None)
    # 删除对应行所有数据，时间戳是等于或大于过去的时间戳即可删除。
    client.deleteAllRowTs('tableName', 'row', timestamp=123312, attributes=None)

    # 返回多行则需要使用scan
    scan = TScan()
    #TScan的属性(检索条件如下)
    #startRow:
    #stopRow:
    #timestamp:
    #columns:
    #caching:
    #filterString: filter
    #sortColumns:
    #filter有如下条件：
    # 限制某个列的值等于26
    filter = "ValueFilter(=,'binary:26')"
    # 值包含6这个值
    filter = "ValueFilter(=,'substring:6')"
    # 列名中的前缀为birthday的
    filter = "ColumnPrefixFilter('birth')"
    # 支持多个过滤条件通过括号、AND和OR的条件组合
    filter = "ColumnPrefixFilter('birth') AND ValueFilter ValueFilter(=,'substring:1987')"
    # 对Rowkey的前缀进行判断
    filter = "PrefixFilter('E')"
    scan.filterString = "ValueFilter(=,'substring:6')"
    id = client.scannerOpenWithScan(tableName='xfyan_test', scan=scan, attributes=None)
    # 返回10行
    result2 = client.scannerGetList(id, 10)
    # scannerGet则是每次只取一行数据
    result = client.scannerGet(id)

    # "不知道row的情况下访问hbase,获取所有的row"
    scanner = client.scannerOpen(table='table_name', startRow=1446708556096, columns=['name','age',], attributes=None)
    scanner.scannerOpenTs(table='table_name', startRow=1446708556096, columns=['name','age',], timestamp='1446708556096', attributes=None)  # 多加一个时间戮参数
    scanner.scannerOpenWithStopTs(tableName='table_name', startRow=1446708556096, stopRow=14467085560999, columns=['name','age',], timestamp='1446708556096',
                                        attributes=None)  # 多加一个stopRow
    r = client.scannerGet(scanner)
    while r:
        r = client.scannerGet(scanner)
        client.scannerClose(scanner)
    client.scannerClose(id)
