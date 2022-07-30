#!/bin/bash

source ${SETUPDIR?}/include/db2_constants
source ${SETUPDIR?}/include/db2_common_functions

crate_cuprod_table()
{
    echo "(*) Creating CUPROD table"
    su - ${DB2INSTANCE?} -c "db2 connect to ${DBNAME?} && db2 -f /var/custom/as400_create_cuprod_table.sql"
}

crate_cuprod_table
