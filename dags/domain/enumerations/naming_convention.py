from domain.enumerations.environment import Environment


class NamingConvention:

    names = {
        Environment.DEV: {
            "user": "dev_user",
            "pass": "dev_pass",
            "host": "dev_host",
            "db_type": "dev_db_type",
            "db_lib": "dev_db_lib",
            "db_port": "dev_db_port",
            "db_schema": "dev_db_schema",
        },
        Environment.STAGE: {
            "user": "stage_developer_username",
            "pass": "stage_developer_password",
            "host": "stage_developer_hostname",
            "db_type": "stage_developer_db_type",
            "db_lib": "stage_developer_db_lib",
            "db_port": "stage_developer_db_port",
            "db_schema": "stage_developer_db_schema",
        },
        Environment.PROD: {
            "user": "prod_user",
            "pass": "prod_pass",
            "host": "prod_host",
            "db_type": "prod_db_type",
            "db_lib": "prod_db_lib",
            "db_port": "prod_db_port",
            "db_schema": "prod_db_schema",
        },
    }
