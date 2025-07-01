from sqlalchemy import create_engine, text
from jproperties import Properties



class connect_db:

    def __init__(self):
        
        self.p = Properties()
        with open("app.properties", 'r+b') as f:

            self.p.load(f)
        
        
        self.uri = self.p.get('uri').data
        self.password = self.p.get('password')

        self.engine = create_engine(self.uri)

        #return self.engine
