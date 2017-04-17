# coding=utf8

DB_INFO = {
    "MYSQL_CONN_INFO": {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "passwd": "123456",
        "db": "testdb",
        "charset": "utf8",
        "connect_timeout": 60 * 60
    },
    "POSTGRESQL_CONN_INFO": {
        "host": "127.0.0.1",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
        "connect_timeout": 60 * 60
    }
}

FILE_SERVER = {
    "server": {
        "server1": {
            "host": "127.0.0.1",
            "port": 22,
            "username": "test",
            "password": "test"
        }
    },
    "remotepath": ["/var/apps/lawson/logs/webapps/access/"],
    "localpath": ["/data/lawson_etl/access_log_etc/"],
    "file_type_conf": {
        "access_log": "access.log.",
        "location_log": "location.log."
    },
    "file_table_mapping": {
        "access_log": "telemetry",
        "location_log": "location"
    }
}

DB_TABLE_STRUCTURE = {
    "FROM-mysql": {
        "USER-GROUP": {
            "users": {
                "_sql_query":
                    "select "
                    "user_id as user_id, "
                    "null as client_user_id, "
                    "gender as gender, "
                    "birthday as birthday, "
                    "create_date as register_date, "
                    "district as district, "
                    "province as province, "
                    "barcode as client_barcode "
                    "from t_user "
                    "where update_date between '{update_date}' and '{max_update_date}' "
            },
            "location": {
                "_sql_query":
                    "select "
                    "user_no as user_id, "
                    "time as timestamp, "
                    "latitude as latitude, "
                    "longitude as longitude "
                    "from t_user_location "
                    "where time between '{time}' and '{max_time}' "
            }
        },
        "SHOP-GROUP": {
            "shops": {
                "_sql_query":
                    "select "
            }
        },
        "PRODUCTS-GROUP": {
            "products": {
                "_sql_query":
                    "select "
            }
        },
        "COUPONS-GROUP": {
            "coupons": {
                "_sql_query":
                    "select "
            }
        },
        "CAMPAIGNS-GROUP": {
            "campaigns": {
                "_sql_query":
                    "select "
            }
        },
        "ORDERS": {
            "receipts": {
                "_sql_query":
                    "select "
            }
        },
        "RELATIONSHIP": {
            "shop_products": {
                "_sql_query":
                    "select "
            }
        }
    },

    "TO-postgresql": {
        "USER-GROUP": {
            "users": {
                "field": ["user_id", "client_user_id", "gender", "birthday", "register_date", "district", "province", "client_barcode"],
                "fieldType": ["int", "str", "int", "str", "str", "str", "str", "str"],
                "delta_table": ["t_user"],
                "primarykey": ["user_id"] # 不知道主键是什么，不知主键有几个，不知道主键在DDL中的位置
            }
        },

        "SHOP-GROUP": {
            "shops": {
                "field": [],
                "fieldType": [],
                "delta_table": [],
                "primarykey": [] # 不知道主键是什么，不知主键有几个，不知道主键在DDL中的位置
            }
        },

        "PRODUCTS-GROUP": {
            "products": {
                "field": [],
                "fieldType": [],
                "delta_table": [],
                "primarykey": [] # 不知道主键是什么，不知主键有几个，不知道主键在DDL中的位置
            }
        },

        "COUPONS-GROUP": {
            "coupons": {
                "field": [],
                "fieldType": [],
                "delta_table": []
            }
        },

        "CAMPAIGNS-GROUP": {
            "campaigns": {
                "field": [],
                "fieldType": [],
                "delta_table": []
            }
        },

        "RELATIONSHIP": {
            "user_coupons": {
                "field": [],
                "fieldType": [],
                "delta_table": []
            }
        },

        "ORDERS": {
            "receipts": {
                "field": [],
                "fieldType": [],
                "delta_table": []
            }
        },

        "FILES": {
            "telemetry": {
                "field": [],
                "fieldType": []
            },
            "location": {
                "field": [],
                "fieldType": [],
                "delta_table": []
            }
        }
    }
}

OTHERS_PARAMS = {
    "UPDATE_TABLES": ["users", "shops", "products"],
    "READ_FILE_MAX_ROWS": 10000,
    "DETAL_NUM": 10000,
    "MAIL_CONF": {
        "FROM_ADDR": "",
        "PASSWORD": "",
        "TO_ADDR": ["", ""],
        "SMTP_SERVER": "smtp.qiye.163.com",
        "PORT": 25,
        "ATTACHMENT": ""
    },
    "LOG_CONFIG": "D:/yoren-project/2017-mysql-To-postgres/lawson_etl/tblEtl/dwetl/plog/logging.conf.dev",
    "ERROR_LOG": "D:/yoren-project/2017-mysql-To-postgres/lawson_etl/tblEtl/logs/etlerror.log",
    "___VERSION__": "0.0.3",
    "LOG_MESSAGE": {
        "TABLE_OR_VIEW": [],
        "INFO": [],
        "ERROR": [],
        "WARNING": []
    }
}

REFRESH_VIEW = {
    "user_tags": "refresh materialized view user_tags; ",
    "receipts_tags": "refresh materialized view receipts_tags; "
}
