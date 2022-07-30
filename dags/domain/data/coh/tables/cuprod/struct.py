CUPROD_TABLE_STRUCTURE = """
                FDAKTR DECIMAL(7,0) DEFAULT 0 NOT NULL,
                FDAVCD CHAR(2) DEFAULT ' ' NOT NULL,
                FDDSGN CHAR(40) DEFAULT ' ' NOT NULL,
                FDAKTX DECIMAL(7,0) DEFAULT 0 NOT NULL,
                FDSUID CHAR(8) DEFAULT ' ' NOT NULL,
                FDPCID CHAR(2) DEFAULT ' ' NOT NULL,
                FDCMFG CHAR(2) DEFAULT ' ' NOT NULL,
                FDLPGC CHAR(4) DEFAULT ' ' NOT NULL,
                FDPSCF CHAR(1) DEFAULT ' ' NOT NULL,
                FDPSC1 DECIMAL(11,2) DEFAULT 0 NOT NULL,
                FDPSC2 DECIMAL(11,2) DEFAULT 0 NOT NULL,
                FDPSC3 DECIMAL(11,2) DEFAULT 0 NOT NULL,
                FDPRUM CHAR(1) DEFAULT ' ' NOT NULL,
                FDGUVC DECIMAL(5,4) DEFAULT 0 NOT NULL,
                FDGPRI DECIMAL(11,2) DEFAULT 0 NOT NULL,
                FDPRGC CHAR(5) DEFAULT ' ' NOT NULL,
                FDVDPC CHAR(3) DEFAULT ' ' NOT NULL,
                FDCHG1 CHAR(3) DEFAULT ' ' NOT NULL,
                FDCHG2 CHAR(3) DEFAULT ' ' NOT NULL,
                FDCHG3 CHAR(3) DEFAULT ' ' NOT NULL,
                FDCHG4 CHAR(3) DEFAULT ' ' NOT NULL,
                FDVAT1 CHAR(1) DEFAULT ' ' NOT NULL,
                FDVAT2 CHAR(1) DEFAULT ' ' NOT NULL,
                FDVAT3 CHAR(1) DEFAULT ' ' NOT NULL,
                FDLPCD CHAR(1) DEFAULT ' ' NOT NULL,
                FDPGCD CHAR(12) DEFAULT ' ' NOT NULL,
                FDCMPG CHAR(2) DEFAULT ' ' NOT NULL,
                FDPSER NUMERIC(5,0) DEFAULT 0 NOT NULL,
                FDPSEQ NUMERIC(5,0) DEFAULT 0 NOT NULL,
                FDMACE CHAR(2) DEFAULT ' ' NOT NULL,
                FDDTYP CHAR(1) DEFAULT ' ' NOT NULL,
                FDPSF3 CHAR(1) DEFAULT ' ' NOT NULL,
                FDOSCL CHAR(1) DEFAULT ' ' NOT NULL,
                FDTXTF CHAR(1) DEFAULT ' ' NOT NULL,
                FDPREC CHAR(3) DEFAULT ' ' NOT NULL,
                FDFESB CHAR(4) DEFAULT ' ' NOT NULL,
                FDFFSB CHAR(1) DEFAULT ' ' NOT NULL,
                FDXDSB CHAR(1) DEFAULT ' ' NOT NULL,
                FDN3CC CHAR(4) DEFAULT ' ' NOT NULL,
                FDA0CE CHAR(4) DEFAULT ' ' NOT NULL,
                FDT4CC CHAR(4) DEFAULT ' ' NOT NULL,
                FDKZCE CHAR(30) DEFAULT ' ' NOT NULL,
                FDZHTB CHAR(15) DEFAULT ' ' NOT NULL,
                FDL020 CHAR(20) DEFAULT ' ' NOT NULL,
                FDLPGM CHAR(10) DEFAULT ' ' NOT NULL,
                FDUSRA CHAR(10) DEFAULT ' ' NOT NULL,
                FDAATS TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"""

CUPROD_COLUMNS = (
    "FDAKTR, "
    "FDAVCD, "
    "FDDSGN, "
    "FDAKTX, "
    "FDSUID, "
    "FDPCID, "
    "FDCMFG, "
    "FDLPGC, "
    "FDPSCF, "
    "FDPSC1, "
    "FDPSC2, "
    "FDPSC3, "
    "FDPRUM, "
    "FDGUVC, "
    "FDGPRI, "
    "FDPRGC, "
    "FDVDPC, "
    "FDCHG1, "
    "FDCHG2, "
    "FDCHG3, "
    "FDCHG4, "
    "FDVAT1, "
    "FDVAT2, "
    "FDVAT3, "
    "FDLPCD, "
    "FDPGCD, "
    "FDCMPG, "
    "FDPSER, "
    "FDPSEQ, "
    "FDMACE, "
    "FDDTYP, "
    "FDPSF3, "
    "FDOSCL, "
    "FDTXTF, "
    "FDPREC, "
    "FDFESB, "
    "FDFFSB, "
    "FDXDSB, "
    "FDN3CC, "
    "FDA0CE, "
    "FDT4CC, "
    "FDKZCE, "
    "FDZHTB, "
    "FDL020, "
    "FDLPGM, "
    "FDUSRA, "
    "FDAATS "
)
