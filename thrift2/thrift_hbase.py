from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol
from hbase import THBaseService
from hbase.ttypes import  *
import os
if __name__ == '__main__':
    os.system('kinit -kt /etc/security/keytabs/xfyan.keytab xfyan/bgs-5p242-yanxufei@BFD.COM')

    socket = TSocket.TSocket('172.24.5.242', 9090)
    #transport = TTransport.TBufferedTransport(socket)
    transport = TTransport.TSaslClientTransport(socket,host='bgs-5p242-yanxufei',service='xfyan',mechanism='GSSAPI')
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = THBaseService.Client(protocol)
    transport.open()


    tablename = "han_test01:hym_hh"
    #tablename="default:xfyan_test"
    rowKey = "001"

    #get
    get = TGet(row=rowKey, columns=[TColumn(family='')])
    result = client.get(tablename,get)
    print  result

    #put
    tablename='xfyan_test'
    rowKey='paper'
    coulumnValue1 = TColumnValue('cf', 'title', 'test')#ColumnFamily，Column，Value
    coulumnValue2 = TColumnValue('cf', 'content', 'hello world')
    coulumnValues = [coulumnValue1, coulumnValue2]
    print 'coulumnValues is ', coulumnValues
    tPut = TPut(rowKey, coulumnValues)
    print 'tPut is ', tPut
    client.put(tablename,tPut)


    #delete
    coulumnValue1 = TColumnValue(family='cf', qualifier=None, value='test', timestamp=None, tags=None)
    coulumnValues = [coulumnValue1]
    tdelete = TDelete(rowKey, coulumnValues)
    print 'tdelete is ', tdelete
    result = client.deleteSingle(tablename, tdelete)
    print result

    transport.close()





