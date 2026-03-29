import psycopg2
import uuid

DB_CONFIG = {
    "host": "host.docker.internal",
    "database": "Body_Performance",
    "user": "etl_worker",
    "password": "MindX15",
    "port": 5432
}

# Tạo bảng log
def setup_tables():
    sql_create_log = """
    CREATE TABLE IF NOT EXISTS etl_log (
        log_id SERIAL PRIMARY KEY,
        run_id VARCHAR(50),
        step_name VARCHAR(50),
        status VARCHAR(20),
        message TEXT,
        log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(sql_create_log)
    conn.commit()

    cur.close()
    conn.close()

# Tạo run_id
RUN_ID = f"RUN_{uuid.uuid4().hex[:8]}"

# Ghi log
def log_step(cur, step_name, status, message=None):
    cur.execute("""
        INSERT INTO etl_log(run_id, step_name, status, message)
        VALUES (%s, %s, %s, %s);
    """, (RUN_ID, step_name, status, message))


# Hàm validate riêng
def validate_data():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        log_step(cur, "VALIDATION", "START", "Checking data...")

        cur.execute("SELECT COUNT(*) FROM raw_body;")
        count = cur.fetchone()[0]

        if count == 0:
            raise Exception("No data in raw_body")

        log_step(cur, "VALIDATION", "SUCCESS", f"Total rows: {count}")

        conn.commit()
        print("Validation passed!")

    except Exception as e:
        conn.rollback()
        conn.autocommit = True
        log_step(cur, "VALIDATION", "FAIL", str(e))
        print("Validation failed:", e)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    setup_tables()
    validate_data()
    log_step()