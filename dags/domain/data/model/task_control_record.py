from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TaskControlRecord(Base):
    __tableNAME__ = "CONTROLE_TRANSACAO_DETALHE"
    CTTD_ID = Column(Integer, primary_key=True)
    CTTR_ID = Column(Integer)
    CTTD_NM = Column(String)
    CTTD_ST = Column(Integer)
    CTTD_HR_DT_START_TRANSACTION = Column(DateTime)
    CTTD_HR_DT_END_TRANSACTION = Column(DateTime)
    CTTD_DT_INSERT = Column(DateTime)
    CTTD_QT_PROCESSED_RECORD = Column(Integer)
    CTTD_USER_INSERT = Column(String)
    CTTD_DT_UPDATE = Column(DateTime)
    CTTD_USER_UPDATE = Column(String)
    CTTD_DT_DELETE = Column(DateTime)
    CTTD_USER_DELETE = Column(String)
