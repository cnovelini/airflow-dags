from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TaskErrorRecord(Base):
    __tablename__ = "ctr_control_transaction_error"
    CTTE_ID = Column(Integer, primary_key=True)
    CTTE_DT_INSERT = Column(DateTime)
    CTTE_USER_INSERT = Column(String)
    CTTE_DT_UPDATE = Column(DateTime)
    CTTE_USER_UPDATE = Column(String)
    CTTE_DT_DELETE = Column(DateTime)
    CTTE_USER_DELETE = Column(String)
    CTTD_ID = Column(Integer)
