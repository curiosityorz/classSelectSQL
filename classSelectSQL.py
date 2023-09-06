#!/usr/bin/env python
# coding: utf-8



#配置文件中db_config
##mysql连接串
MYSQL_CONFIG = {
    
    'host':'xxxx',
    'user':'xxxx',
    'passwd':'xxxx',
    'port':xxxx,
    'database':'xxxx',
    'charset':'xxxx',
    'engine':'mysql'
}

#Oracle连接串
ORA_CONFIG = {
    'connect':'xxx/xxxx@xxxx/xxxx',
    'engine':'oracle'
}


class Handle_sql:
    import numpy as np
    import pandas as pd
    import logging
    import traceback
    import time 
    import os
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 解决编码格式问题
    
    filename1=os.path.join(os.getcwd(),'Handle_sql.log')
    #默认日志打印级别为warning，日志等级按级别排序：DEBUG<INFO<WARNING<ERROR<CRITICAL，日志信息量依次减少
    logging.basicConfig(level='ERROR',filename=filename1, filemode='a')
    
    def __init__(self,db_config):
        self.config = db_config.copy()
        self.engine = self.config.pop('engine')
    
    #数据库连接
    def get_con(self):
        #如果engine没有值默认返回mysql
        if self.engine.lower() == 'mysql':
            import pymysql
            self.conn = pymysql.connect(**self.config)
            self.cursor = self.conn.cursor()
        elif self.engine.lower() == 'oracle':
            import cx_Oracle
            strConnect = self.config['connect']
            self.conn = cx_Oracle.connect(strConnect)
            self.cursor = self.conn.cursor()
        else:
            pass
    
    #关闭连接
    def close(self):
        self.cursor.close()
        self.conn.close()
    
    #数据查询   
    def get_fetchall(self,sql,*args1):
        '''*args:传入sql的参数,用于报错时打印异常信息'''
        #返回DataFrame
        try:
            self.get_con()
            self.cursor.execute(sql)
            res_1 = pd.DataFrame(list(self.cursor.fetchall()), columns=[i[0] for i in self.cursor.description])
            return res_1
            self.close()
        except Exception as e:
            print(e)
            logging.error(time.strftime('%Y-%m-%d %H:%M:%S') + ' ' + ' ' + traceback.format_exc())
    
    #用表号或ID等明细查SQL
    def selectSQLmanyID(self,sql,step=1000,*arg1):
        '''
        sql：SQL语句;
        lst_x：需要查询的ID列表，比如表号的列表,它需要在*arg1中很多参数中排最后，才能进行自动拆分循环查询;
        step：步长，一次查多少ID;

        '''
        arg2 = []
        for arg in arg1:
            arg2.append(arg)
        #if len(arg2) > 1:
        arg2.remove(arg)
        
        lst_x = arg
        df_x = pd.DataFrame()
        for i in  range(0,len(tuple(lst_x)),step): 
            print(i)
            start = time.time()
            try:
                #print('arg2:{}'.format(arg2))
                arg2_ = arg2.copy()
                lst_x_p1 = str(lst_x[i:i+step])[1:-1]
                #print('lst_x_p1:{}'.format(lst_x_p1))
                
                #print('(0)arg2_:{}'.format(arg2_))
                arg2_.append(lst_x_p1)
                sql_ = sql.format(*arg2_)
                #print('arg2_:{}'.format(arg2_))
                #print(sql_)
                res1 = self.get_fetchall(sql_)
                df_x = pd.concat([res1, df_x])
            except Exception as e:
                print(e)
                logging.error(time.strftime('%Y-%m-%d %H:%M:%S') + ' ' + ' ' + traceback.format_exc())
            end = time.time()
            print('花费时间：%sS'%(end-start))
        return df_x
    
    #数据插入与修改
    def loop_insert(self, sql_insert,*args1):
        count = 0
        try:
            self.get_con()
            count = self.cursor.execute(sql_insert,*args1)
            self.conn.commit()
            self.close()
            #if pd.isnull(result1):
                #count = 1
            print("插入成功")
        except Exception as e:
            print("操作失败！" + str(e))
            logging.error(time.strftime('%Y-%m-%d %H:%M:%S') + traceback.format_exc())
            self.conn.rollback()
        #return count
    
    #把DataFrame插入数据库
    def insert_df(self,df:"dataframe",table_name:"数据库中的数据表"):
        sql_insert = """insert into {} values ({})"""
        self.lst_state = []
        for i in range(df.shape[0]):
            row = str(df.iloc[i].tolist())[1:-1]
            sql_insert_ = sql_insert.format(table_name,row)
            print('已插入第{}行'.format(i))
            num_return = self.loop_insert(sql_insert_)
            num_return = 1
            ##返回每一行是否插入成功
            self.lst_state.append(num_return)
            lst_num = list(range(len(self.lst_state)))
            data_state = pd.DataFrame(zip(lst_num,self.lst_state))
        return data_state
        

    #使用del query，会关闭数据库连接
    #__del__ 为魔法方法，python在回收垃圾对象之前，会调用该对象的__del__()方法
    def __del__(self):
        self.conn.close()
        #手动关闭日志记录过程
        logging.shutdown()

