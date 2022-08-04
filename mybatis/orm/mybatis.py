from abc import ABCMeta, abstractmethod
import oracledb as oracle
import os


class Visitor():
    '''
    数据库访问抽象层
    '''

    def query(self) -> list:
        '''
        查询数据表结构
        '''
        raise NotImplementedError()

    def generateModel(self, describe, records) -> None:
        '''
        生成模型
        '''
        self.model = []
        for record in records:
            field = {}
            for index, item in enumerate(record):
                field[describe[index][0]] = item
            self.model.append(field)

    def getJavaType(self, type: str, length: int, precision: int) -> str:
        pass

    def getModel(self, refresh: bool = False) -> list:
        if hasattr(self, 'model') == False or refresh:
            self.generateModel(* self.query())
        return self.model


class OracleVisitor(Visitor):
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    oracle.init_oracle_client()

    def __init__(self, host: str, port: int, user: str, password: str, service: str, sql: str) -> None:
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.service = service
        self.sql = sql

    def query(self) -> list:
        with oracle.connect(
                user=self.user, password=self.password, host=self.host, port=self.port, service_name=self.service) as db:
            with db.cursor() as cursor:
                cursor.execute(self.sql)
                return cursor.description, cursor.fetchall()

    def getJavaType(self, type: str, length: int, precision: int) -> str:
        index = type.find('(')
        name = (type if index == -1 else type[:index]).upper()
        if 'VARCHAR2' == name:
            return 'String'
        elif 'TIMESTAMP' == name:
            return 'Date'
        elif 'NUMBER' == name:
            return 'Double' if precision is not None else 'Long' if length >= 10 else 'Integer'


class Generator:
    def __init__(self, visitor: Visitor):
        self.visitor = visitor
        self.pojo_fields = ''
        self.pojo_methods = ''
        self.insert_columns = ''
        self.insert_values = ''
        self.where = ''
        self.update = ''
        self.id_column = ''
        self.id_type = ''

    def getJavaName(self, name: str) -> str:
        '''
        获取pojo的属性名字
        '''
        list = name.split('_')
        return list.pop(0).lower() + ''.join([i.lower().capitalize() for i in list])

    def process(self):
        '''
        处理数据模型获取相关信息
        '''
        model = self.visitor.getModel()

        id_field = model[0]
        self.id_column = id_field['COLUMN_NAME']
        self.id_name = self.getJavaName(self.id_column)
        self.id_type = self.visitor.getJavaType(id_field['DATA_TYPE'],
                                                id_field['DATA_LENGTH'], id_field['DATA_PRECISION'])

        for index, field in enumerate(model):
            type = self.visitor.getJavaType(field['DATA_TYPE'],
                                            field['DATA_LENGTH'], field['DATA_PRECISION'])
            name = self.getJavaName(field['COLUMN_NAME'])
            self.pojo_fields += '\tprivate {type} {name};\r'.format(
                type=type, name=name)
            self.pojo_methods += '\tpublic {type} get{cname}(){{\r\t\treturn this.{name};\r\t}}\r\r'.format(
                type=type, cname=name.capitalize(), name=name)
            self.pojo_methods += '\tpublic void set{cname}({type} {name}) {{\r\t\tthis.{name} = {name};\r\t}}\r\r'.format(
                type=type, cname=name.capitalize(), name=name)
            self.insert_columns += ', {}'.format(field['COLUMN_NAME'])
            self.insert_values += ', {}'.format(name)
            self.where += '\r\t\t\t<if test="params.{name} != null and params.{name} != \'\'">\r\t\t\t\tand t.{column} = #{{params.{name}}}\r\t\t\t</if>'.format(
                name=name, column=field['COLUMN_NAME'])
            self.update += '\r\t\t\t<if test="{name} != null">\r\t\t\t\t{column} = #{{{name}}},\r\t\t\t</if>'.format(
                name=name, column=field['COLUMN_NAME'])

        self.insert_columns = self.insert_columns[2:]
        self.insert_values = self.insert_values[2:]


"""
读取输入
"""
# print("数据库配置!")
# host = input("主机或IP地址:")
# port = int(input("端口号:"))
# serviceName = input("服务名:")
# user = input("用户名:")
# password = input("密码:")
# tableName = input("表名字:")
# print('文件路径设置!')
# templateDir = input('模板路径:')
# targetDir = input('输出文件存放路径:')
# print('pojo配置')
# beanPackageName = input('pojo包名:')
# beanName = input('pojo类名:')
# print('dao配置')
# daoPackageName = input('dao包名:')

user = 'ebankdev'
password = 'password'
host = '192.168.0.65'
port = 1521
serviceName = 'orcl'
tableName = 'MDS_HANDLE'
beanPackageName = 'cn.lx.entity'
beanName = 'Handle'
daoPackageName = 'cn.lx.mapper'
templateDir = 'F:/temp/py/orm'
targetDir = 'F:/temp/py/orm/output'

templateDir = templateDir if templateDir[-1] not in [
    '\\', '/'] else templateDir[0:-1]
targetDir = targetDir if targetDir[-1] not in ['\\', '/'] else targetDir[0:-1]
daoName = beanName + 'Mapper'
beanParamName = beanName[0].lower() + beanName[1:]
beanDir = targetDir + '/' + beanPackageName.replace('.', '/')
daoDir = targetDir + '/' + daoPackageName.replace('.', '/')
mapperDir = targetDir + '/'

"""
读取SQL模板
"""
with open(templateDir + '/oracle.sql') as f:
    sql = f.read().format(tableName=(tableName if tableName.find(
        '.') != -1 else user + '.' + tableName))
    generator = Generator(OracleVisitor(
        host=host, port=port, user=user, password=password, service=serviceName, sql=sql))

generator.process()

"""
生成JAVA BEAN代码
"""
os.makedirs(beanDir, exist_ok=True)
with open(beanDir + '/' + beanName + '.java', 'w') as bf:
    with open(templateDir + '/entity.java') as f:
        bf.write(f.read().format(beanPackageName=beanPackageName, beanName=beanName,
                 fields=generator.pojo_fields, getset=generator.pojo_methods))

"""
生成DAO代码
"""
os.makedirs(daoDir, exist_ok=True)
with open(daoDir + '/' + daoName + '.java', 'w') as bf:
    with open(templateDir + '/mapper.java') as f:
        bf.write(f.read().format(beanPackageName=beanPackageName, beanName=beanName, beanParamName=beanParamName, daoPackageName=daoPackageName, daoName=daoName,
                 table_name=tableName, idColumn=generator.id_column, idType=generator.id_type, insert_columns=generator.insert_columns, insert_values=generator.insert_values))

"""
生成Mapper代码
"""
os.makedirs(mapperDir, exist_ok=True)
with open(mapperDir + '/' + daoName + '.xml', 'w') as bf:
    with open(templateDir + '/oracle.xml') as f:
        bf.write(f.read().format(beanName=beanName, daoPackageName=daoPackageName, daoName=daoName, table_name=tableName,
                 where=generator.where, update=generator.update, id_column=generator.id_column, id_name=generator.id_name))
