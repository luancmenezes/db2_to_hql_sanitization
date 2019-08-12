import sqlparse
import re
import sys
import os
import json

from datetime import datetime

class Sanitization:
    def __init__(self,path):
        self.names = list()
        self.types = list()
        self.path = path
        self.tokens = list()
        self.raw = list()
    def readCompilationCtr(self):
        file_ = open(self.path, "r")
        self.raw = file_.read().split(";")
        self.raw.reverse()
    def extractTokens(self,tokens):
        return tokens[0].value,tokens[1],None
    def ctr2def(self,token):
        return token.value[1:-1]
    def parseTokens(self):
        parsed = sqlparse.parse(self.raw.pop())
        self.tokens = [sql for sql in parsed[0].tokens if sql.is_group]
        return self.extractTokens(self.tokens)
    def parse_identifier_list(self,token):
        tk = tokensUtils()
        token = token.split()
        _type = sqlparse.keywords.KEYWORDS.get(token[0])
        if tk.is_keyword(_type):
            _token = token[-1].strip()
            self.names.append(_token)
        else:
            _token = token[-1].strip()
            _type =  token[0].strip()
            self.names.append(_token)
            self.types.append(_type)

class Sanitization_2:
    def __init__(self,path):
        self.db = list()
        self.tb = list()
        self.retention = list()
        self.label = 'PROCESSAR'
        self.path = path
        self.file = None
    def extractTokens(self,tokens):
        if tokens[3].strip() == self.label:
            self.db.append(tokens[0].upper())
            self.tb.append(tokens[1].upper())
            self.retention.append(tokens[2])
    def parseTokens(self):
        file_ = open(self.path, "r")
        for line in file_.readlines():
            self.extractTokens(line.split(';'))
    def concatFile(self, line):
        self.file += line

        
class tokensUtils:
    global KEYWORDS_HQL 
    KEYWORDS_HQL = {
        'CHAR':'string',
        'VARCHAR':'string',
        'DATE':'string',
        'CHARACTER':'string',
        'SMALLINT':'INT',
        'INT':'integer'
    }
    def __init__(self):
        self.keywords = KEYWORDS_HQL
        self.datastore = dict()
        self.list_datastore = None
    def is_identifier_list(self,token):
        return isinstance(token, sqlparse.sql.IdentifierList) 

    def is_function(self,token):
        return isinstance(token, sqlparse.sql.Function) 

    def is_identifier(self,token):
        return isinstance(token, sqlparse.sql.Identifier)
    def is_keyword(self,token):
        if token == sqlparse.tokens.Keyword:
            return True 
        return False
    def is_date(self, token):
        if token == 'DATE':
            return True
        return False
    # def there_is_keyword(self,_keyword,_dict):
    #     if _keyword != _dict['keyword']:
    #         return True
    #     return False
    def parse_HQL(self,_keyword):
        for key in self.keywords.keys():
            _init = _keyword.find(key)
            if _init > -1:
                _keyword = re.sub(r'\([\d]*\)', '', _keyword)
                try:
                    return self.keywords[_keyword]
                except KeyError:
                    # data['keyword'] = list()
                    print('Error: _keyword - {} not defined'.format(_keyword))
                    with open('_keywords', 'rw') as f:
                        self.datastore = json.loads(f.read())
                        if not self.datastore.get(_keyword):
                            self.datastore[_keyword] = True
                            json.dump(self.datastore, f)
                    f.close()
                    exit()
                    ## Falta finalizar - Quando recebe keyWord como Execeção, altualmente para porém não Pode
            else:
                return _keyword
class WriteFiles:
    global HQL_SCRIPT
    
    def __init__(self,_def=None,hql=str(),_tb=None,_db=None):
        self._def = _def
        self.hql = hql
        self.tb = _tb
        self.db = _db
    def _write(self,_type):
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        paths = ['SANITIZACAO','MP']
        if _type == 'def':
            for path in paths:
                dirName = "output/{}/{}".format(path,self.tb.split('.')[1])
                if not os.path.exists(dirName):
                    os.mkdir(dirName)
                _file = dirName +"/{}.{}".format(self.tb.split('.')[1],_type)
                f = open(_file, "w")
                f.write(self._def)
                f.close()
        else:
            HQL_SCRIPT = """CREATE EXTERNAL TABLE IF NOT EXISTS {}(\n{}),\n PARTITIONED BY (\n dat_ref_carga string\n),\nROW FORMAT SERDE\n'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\nSTORED As INPUTFORMAT\n '\torg.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT\n'\torg.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\nTBLPROPERTIES ('parquet.compression'='SNAPPY');""".format(self.tb,self.hql)
            for path in paths:
                dirName = "output/{}/{}".format(path,self.tb.split('.')[1])
                if not os.path.exists(dirName):
                    os.mkdir(dirName)
                _file = dirName +"/{}.{}".format(self.tb.split('.')[1],_type)
                f = open(_file, "w")
                f.write(HQL_SCRIPT)
                f.close()
    def _HQL(self,*args):
        while args[0].names:
            _hql = args[1].parse_HQL(args[0].types.pop())            
            self.hql += args[0].names.pop() + ' ' + _hql + ',\n'
    def _checkFile(self,path):
        return path.exists(path)
        
def main_d_h(path):
    s = Sanitization(path)
    s.readCompilationCtr()
    tk = tokensUtils()
    # try:
    while s.raw:
        _tb,_def,_db = s.parseTokens()
        w = WriteFiles(_def=s.ctr2def(_def),_tb=_tb,_db=_db)
        for token in _def.tokens:
            if token.is_group or tk.is_date(token.value):
                if tk.is_identifier(token):
                    s.names.append(token.value)
                elif tk.is_function(token):
                    s.types.append(token.value)
                elif tk.is_identifier_list(token):
                    s.parse_identifier_list(token.value)
                else:
                    s.types.append(token.value)
        w._HQL(s,tk)
        w._write('def')
        w._write('hql')
    # except AttributeError:
    #     print("AttributeError: {}\n Remaining elements: {}".format(s.raw,len(s.raw)))

def main_c(path):
    s = Sanitization_2(path)
    s.parseTokens()
    listOfLines = list()
    while s.db:
        database = s.db.pop()
        tabela = s.tb.pop()
        retention = s.retention.pop()
        input_create = "/sistemas/bdi/" + database + "/" + tabela + "/INPUT/"
        input_create = input_create.split('\n')
        input_create = ''.join(input_create)
        parquet_create = "/sistemas/bdi/" + database + "/" + tabela + "/parquet/"
        parquet_create = parquet_create.split('\n')
        parquet_create = ''.join(parquet_create)
        cfg_create = "/sistemas/bdf/" + database + "/cfg/"
        cfg_create = cfg_create.split('\n')
        cfg_create = ''.join(cfg_create)
        listOfLines.extend((input_create, parquet_create, cfg_create))
        fileName = tabela + '.conf'
        fileName = fileName.split('\n')
        fileName = ''.join(fileName)
        paths = ['SANITIZACAO','MP']
        for path in paths:
            dirName = "output/{}/{}".format(path,tabela.upper())
            if not os.path.exists(dirName):
                print(dirName)
                os.mkdir(dirName)
            with open (dirName +"/" +fileName, 'w') as conf:
                conf.write("retention="+retention+'\n')
                conf.write("hdfs_path_source=" + input_create + '\n')
                conf.write("hdfs_path_save=" + parquet_create + '\n')
                conf.write("os_path_source=/produtos/bdr/mf/" + '\n')
                conf.write("file_count=1\n")
                conf.write("database=" + database + '\n')
                conf.write('quote="\n')
                conf.write("demiliter=;\n")
                conf.write("header=false")


if __name__ == "__main__":
    main_d_h('compilado_2.txt')
    # if scys.argv[1] == '1':
    #     main_d_h('compilado.txt')
    # else:
    #     main_c(sys.argv[2])
			