"""
One-time setup script: creates the pipeline_user and news_pipeline database
on the local Postgres instance. Run as: python setup_db.py
"""
import sys
import getpass

try:
    import psycopg2
except ImportError:
    print("Installing psycopg2-binary...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])
    import psycopg2

# ── connect as postgres superuser ──────────────────────────────────────────────
postgres_password = getpass.getpass("Enter your local postgres superuser password: ")

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password=postgres_password,
        dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("CREATE USER pipeline_user WITH PASSWORD 'pipeline_pass';")
        print("✔  User 'pipeline_user' created.")
    except psycopg2.errors.DuplicateObject:
        print("ℹ  User 'pipeline_user' already exists — skipping.")

    try:
        cur.execute("CREATE DATABASE news_pipeline OWNER pipeline_user;")
        print("✔  Database 'news_pipeline' created.")
    except psycopg2.errors.DuplicateDatabase:
        print("ℹ  Database 'news_pipeline' already exists — skipping.")

    cur.execute("GRANT ALL PRIVILEGES ON DATABASE news_pipeline TO pipeline_user;")
    print("✔  Grants applied.")

    cur.close()
    conn.close()
    print("\n✅  Postgres setup complete!")

except psycopg2.OperationalError as e:
    print(f"\n❌  Could not connect to Postgres: {e}")
    print("    Make sure Postgres is running and the password is correct.")
    sys.exit(1)
