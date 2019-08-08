import sqlparse
import re
import sys
from datetime import datetime

class Sanitization:
    def __init__(self,path):
        self.names = list()
        self.types = list()
        self.path = path
        self.tokens = list()
    def extractTokens(self,tokens):
        return tokens[0].value,tokens[1],None
    def ctr2def(self,token):
        return token.value[1:-1]
    def parseTokens(self):
        file_ = open(self.path, "r")
        raw = file_.read()
        parsed = sqlparse.parse(raw)[0]
        self.tokens = [sql for sql in parsed.tokens if sql.is_group]
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
        
class tokensUtils:
    global KEYWORDS_HQL 
    KEYWORDS_HQL = {
        'CHAR':'string',
        'VARCHAR':'string',
        'DATE':'string',
        'CHARACTER':'string'
    }
    def __init__(self):
        self.keywords = KEYWORDS_HQL
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
    def parse_HQL(self,_keyword):
        for key in self.keywords.keys():
            _init = _keyword.find(key)
            if _init > -1:
                _keyword = re.sub(r'\([\d]*\)', '', _keyword)
                try:
                    return self.keywords[_keyword]
                except KeyError:
                    print('Error: _keyword - {} not defined'.format(_keyword))
                    exit()
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
        _file = "table_{}.{}".format(current_time,_type)
        f = open(_file, "w")
        if _type == 'def':
            f.write(self._def)
            f.close()
        else:
            HQL_SCRIPT = """CREATE EXTERNAL TABLE IF NOT EXISTS {}(\n{}),\n PARTITIONED BY (\n dat_ref_carga string\n),\nROW FORMAT SERDE\n'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\nSTORED As INPUTFORMAT\n 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT\n'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\nTBLPROPERTIES ('parquet.compression'='SNAPPY'),;""".format(self.tb,self.hql)
            _file = "table_{}.{}".format(current_time,_type)
            f = open(_file, "w")
            f.write(HQL_SCRIPT)
            f.close()
    def _HQL(self,*args):
        while args[0].names:
            _hql = args[1].parse_HQL(args[0].types.pop())            
            self.hql += args[0].names.pop() + ' ' + _hql + ',\n'
    def _checkFile(self,path):
        return path.exists(path)
        
def main(path):
    s = Sanitization(path)
    tk = tokensUtils()
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
     
if __name__ == "__main__":
    [main(ctr) for ctr in sys.argv[1:]]  
			