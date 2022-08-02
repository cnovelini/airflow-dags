from enum import Enum


class DbInsertionMethod(Enum):
    FULL_PD_TO_SQL = 1
    LINE_WISE_PD_TO_SQL = 2
