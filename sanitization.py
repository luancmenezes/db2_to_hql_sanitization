import sqlparse
class Sanitization:
    def __init__(self,path):
        self.names = list()
        self.types = list()
        self.path = path
        self.tokens = list()
    def extractTokens(tokens):
        return tokens[0].value,tokens[1],tokens[2].value
    def ctr2def(token):
        return token.value[1:-1]
    def parseTokens(self)
        file_ = open(self.path, "r")
        raw = file_.read()
        parsed = sqlparse.parse(raw)[0]
        self.tokens = [sql for sql in parsed.tokens if sql.is_group]  
        return extractTokens(self.tokens)
    
    def parse_identifier_list(self,token):
    #     if is_identifier_list(token):


    #sqlparse.keywords.KEYWORDS.get('REAL')
class tokensUtils:
    def is_identifier_list(token):
        return isinstance(token, sqlparse.sql.IdentifierList) 

    def is_function(token):
        return isinstance(token, sqlparse.sql.Function) 

    def is_identifier(token):
        return isinstance(token, sqlparse.sql.Identifier) 

def main()
    s = Sanitization('create.ctr')
    _tb,_def,_db = Sanitization.parseTokens()
    ## write def = Sanitization.ctr2def(_def)
    for token in _def.tokens:
        if token.is_group:
            if tokensUtils.is_identifier(token):
                x.names.append(token.value)
            elif tokensUtils.is_function(token):
                x.types.append(token.value)
            else:
                Sanitization.parse_identifier_list(token.value)
    
        # if sqlparse.keywords.KEYWORDS.get(x.get_name()) != None
        #     value = x.get_name()
if __name__ == "__main__":
    main()  
			