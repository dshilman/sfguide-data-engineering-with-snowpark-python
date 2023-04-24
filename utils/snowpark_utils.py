from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
from typing import Optional

load_dotenv()

# Class to store a singleton connection option
class SnowflakeConnection(object):
    _connection = None

    @property
    def connection(self) -> Optional[Session]:
        return type(self)._connection

    @connection.setter
    def connection(self, val):
        type(self)._connection = val

# Function to return a configured Snowpark session
def get_snowpark_session() -> Session:
    # if running in snowflake
    if SnowflakeConnection().connection:
        # Not sure what this does?
        session = SnowflakeConnection().connection
    # if running locally with a config file
    # TODO: Look for a creds.json style file. This should be the way all snowpark
    # related tools work IMO
    # if using snowsql config, like snowcli does
    else:
        snowpark_config = {
            "account": os.getenv("SNOWSQL_ACCOUNT"),
            "user": os.getenv("SNOWSQL_USER"),
            "password": os.getenv("SNOWSQL_PWD"),
            "role": os.getenv("SNOWSQL_ROLE"),
            "warehouse": os.getenv("SNOWSQL_WAREHOUSE"),
            "database": os.getenv("SNOWSQL_DATABASE"),
            #"schema": os.getenv("SNOWSQL_SCHEMA")
        }    
        SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()

    # elif os.path.exists('C:/Users/ShilmanD/.snowsql'):
    #     snowpark_config = get_snowsql_config()
    #     SnowflakeConnection().connection = Session.builder.configs(snowpark_config).create()
    # # otherwise configure from environment variables

    if SnowflakeConnection().connection:
        return SnowflakeConnection().connection  # type: ignore
    else:
        raise Exception("Unable to create a Snowpark session")


# Mimic the snowcli logic for getting config details, but skip the app.toml processing
# since this will be called outside the snowcli app context.
# TODO: It would be nice to get rid of this entirely and always use creds.json but
# need to update snowcli to make that happen
def get_snowsql_config(
    connection_name: str = 'dev',
    config_file_path: str = os.path.expanduser('C:/Users/ShilmanD/.snowsql')) -> dict:
    import configparser

    snowsql_to_snowpark_config_mapping = {
        'account': 'account',
        'accountname': 'account',
        'username': 'user',
        'password': 'password',
        'rolename': 'role',
        'warehousename': 'warehouse',
        'dbname': 'database',
        'schemaname': 'schema'
    }
    config = configparser.ConfigParser(inline_comment_prefixes="#")
    connection_path = 'connections.' + connection_name

    config.read(config_file_path)
    session_config = config[connection_path]
    # Convert snowsql connection variable names to snowcli ones
    session_config_dict = {
        snowsql_to_snowpark_config_mapping[k]: v.strip('"')
        for k, v in session_config.items()
    }
    return session_config_dict

