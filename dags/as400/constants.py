from as400.utils import generate_profiles_by_environment


COMMIT_MODE_NONE = 0  # Commit immediate (*NONE)   --> QSQCLIPKGN
COMMIT_MODE_CS = 1  # Read committed (*CS)       --> QSQCLIPKGS
COMMIT_MODE_CHG = 2  # Read uncommitted (*CHG)    --> QSQCLIPKGC
COMMIT_MODE_ALL = 3  # Repeatable read (*ALL)     --> QSQCLIPKGA
COMMIT_MODE_RR = 4  # Serializable (*RR)         --> QSQCLIPKGL

CONNECTION_TYPE_READWRITE = 0  # Read/Write (all SQL statements allowed)
CONNECTION_TYPE_CALL = 1  # Read/Call (SELECT and CALL statements allowed)
CONNECTION_TYPE_READONLY = 2  # Read-only (SELECT statements only)

AS400_PROFILES = generate_profiles_by_environment()
