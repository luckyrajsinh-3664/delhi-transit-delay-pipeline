import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

KEY_DIR = Path("keys")
PRIVATE_KEY = KEY_DIR / "snowflake_rsa_key.pem"
SETUP_SQL = KEY_DIR / "snowflake_user_setup.sql"

SQL_TEMPLATE = """-- Run once in Snowsight (Run All). Creates the loader role + service user.
-- Contains only the PUBLIC key, which is safe to paste into Snowflake.

-- 1) role and user (USERADMIN manages roles and users)
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS ROUTERADAR_LOADER;
GRANT ROLE ROUTERADAR_LOADER TO ROLE SYSADMIN;

CREATE USER IF NOT EXISTS ROUTERADAR_LOADER_SVC
    TYPE = SERVICE
    DEFAULT_ROLE = ROUTERADAR_LOADER
    DEFAULT_WAREHOUSE = ROUTERADAR_WH
    DEFAULT_NAMESPACE = ROUTERADAR.DW
    COMMENT = 'RouteRadar loader (key-pair login, used by warehouse/load_snowflake.py)';
ALTER USER ROUTERADAR_LOADER_SVC SET RSA_PUBLIC_KEY = '{public_key}';
GRANT ROLE ROUTERADAR_LOADER TO USER ROUTERADAR_LOADER_SVC;

-- 2) privileges (SYSADMIN owns the warehouse, database and tables)
USE ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE ROUTERADAR_WH TO ROLE ROUTERADAR_LOADER;
GRANT USAGE ON DATABASE ROUTERADAR TO ROLE ROUTERADAR_LOADER;
-- CREATE TABLE/STAGE/FILE FORMAT: the loader uploads into temporary staging tables
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA ROUTERADAR.DW TO ROLE ROUTERADAR_LOADER;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ROUTERADAR.DW TO ROLE ROUTERADAR_LOADER;

-- 3) check: should list the key fingerprint (RSA_PUBLIC_KEY_FP starts with SHA256:)
USE ROLE USERADMIN;
DESC USER ROUTERADAR_LOADER_SVC;
"""


def main():
    if PRIVATE_KEY.exists():
        sys.exit(f"{PRIVATE_KEY} already exists - not overwriting it. "
                 f"Delete it first only if you really want a new key.")
    KEY_DIR.mkdir(exist_ok=True)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    PRIVATE_KEY.write_bytes(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    pub_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    # Snowflake wants the base64 body only: no BEGIN/END lines, no line breaks
    pub_body = "".join(l for l in pub_pem.splitlines() if "-----" not in l)
    SETUP_SQL.write_text(SQL_TEMPLATE.format(public_key=pub_body))

    print(f"Private key : {PRIVATE_KEY}  (keep it secret, it is gitignored)")
    print(f"Setup SQL   : {SETUP_SQL}  (open it, copy all, Run All in Snowsight)")


if __name__ == "__main__":
    main()