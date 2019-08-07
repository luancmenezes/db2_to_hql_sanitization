import sqlparse
import re
class Sanitization:
    def __init__(self,path):
        self.names = list()
        self.types = list()
        self.path = path
        self.tokens = list()
    def extractTokens(self,tokens):
        return tokens[0].value,tokens[1],tokens[2].value
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
        'CHAR':'STRING',
        'VARCHAR':'STRING',
        'DATE':'STRING'
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
                # _key = _keyword[_init:len(key)]
                return self.keywords[_keyword]
            else:
                return _keyword
class WriteFiles:
    def __init__(self,_def=None,hql=str()):
        self._def = _def
        self.hql = hql
    def _write(self,_type):
        _file = "table.{}".format(_type)
        f = open(_file, "w")
        if _type == 'def':
            f.write(self._def)
        else:
            f.write(self.hql)
        f.close()
    def _HQL(self,*args):
        while args[0].names:
            _hql = args[1].parse_HQL(args[0].types.pop())            
            self.hql += args[0].names.pop() + ' ' + _hql + ',\n'
        


def main():
    s = Sanitization('create.ctr')
    tk = tokensUtils()
    _tb,_def,_db = s.parseTokens()
    w = WriteFiles(s.ctr2def(_def))
    # w._write('def')
    ## write def = 
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
    print(w.hql)
        # if sqlparse.keywords.KEYWORDS.get(x.get_name()) != None
        #     value = x.get_name()
if __name__ == "__main__":
    main()  
			