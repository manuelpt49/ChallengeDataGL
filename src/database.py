import sqlalchemy as _sql
import sqlalchemy.ext.declarative as _declarative
import sqlalchemy.orm as _orm
import pymysql

SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:admin@localhost/challenge"

engine = _sql.create_engine(SQLALCHEMY_DATABASE_URL)

sessionLocal = _orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)

base = _declarative.declarative_base()