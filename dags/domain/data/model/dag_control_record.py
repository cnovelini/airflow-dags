from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DagControlRecord(Base):
    __tableNAME__ = "CONTROLE_TRANSACAO"
    CTTR_ID = Column(Integer, primary_key=True)
    CTTR_NM = Column(String)
    CTTR_ST = Column(Integer)
    CTTR_HR_DT_START_PROCESS = Column(DateTime)
    CTTR_HR_DT_END_PROCESS = Column(DateTime)
    CTTR_QT_PROCESSED_RECORD = Column(Integer)
    CTTR_DT_INSERT = Column(DateTime)
    CTTR_USER_INSERT = Column(String)
    CTTR_DT_UPDATE = Column(DateTime)
    CTTR_USER_UPDATE = Column(String)
    CTTR_DT_DELETE = Column(DateTime)
    CTTR_USER_DELETE = Column(String)
