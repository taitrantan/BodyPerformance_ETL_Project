import psycopg2
import uuid

DB_CONFIG = {
    "host": "host.docker.internal",
    "database": "Body_Performance",
    "user": "etl_worker",
    "password": "MindX15",
    "port": 5432
}

# Tạo run_id
RUN_ID = f"RUN_{uuid.uuid4().hex[:8]}"


def log_step(cur, step_name, status, message=None):
    cur.execute("""
        INSERT INTO etl_log(run_id, step_name, status, message)
        VALUES (%s, %s, %s, %s);
    """, (RUN_ID, step_name, status, message))


def setup_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            log_id SERIAL PRIMARY KEY,
            run_id VARCHAR(50),
            step_name VARCHAR(50),
            status VARCHAR(20),
            message TEXT,
            log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

# MAIN ETL + VALIDATION
def validate_data():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        log_step(cur, "PIPELINE", "START", "Begin ETL run")

        # LOAD DW      
        log_step(cur, "LOAD_DW", "START", "Aggregating data into dw_body")
      
        sql_load = """
        WITH aggregated AS (
            SELECT
                gender,
                class,
                AVG(age) AS avg_age,
                AVG(height_cm) AS avg_height,
                AVG(weight_kg) AS avg_weight,
                AVG(bmi) AS avg_bmi,
                AVG(gripforce) AS avg_grip,
                AVG(sit_ups) AS avg_situps
            FROM clean_body
            GROUP BY gender, class
        )
        INSERT INTO dw_body(
            gender, class, avg_age, avg_height, avg_weight, avg_bmi, avg_grip, avg_situps
        )
        SELECT
            gender, class, avg_age, avg_height, avg_weight, avg_bmi, avg_grip, avg_situps
        FROM aggregated;
        """

        cur.execute(sql_load)

        log_step(cur, "LOAD_DW", "SUCCESS", "DW load completed")

        # VALIDATION
        # =========================
        log_step(cur, "VALIDATION", "START", "Checking DW consistency")

        cur.execute("SELECT COUNT(*) FROM dw_body;")
        count_dw = cur.fetchone()[0]

        if count_dw == 0:
            raise RuntimeError("Validation failed: dw_body is empty")

        cur.execute("""
            SELECT COUNT(DISTINCT gender, class) FROM clean_body;
        """)
        expected = cur.fetchone()[0]

        if count_dw != expected:
            raise RuntimeError(f"Expected {expected}, got {count_dw}")

        log_step(cur, "VALIDATION", "SUCCESS", "DW data is valid")
      
        # SUCCESS
        # =========================
        log_step(cur, "PIPELINE", "SUCCESS", "ETL completed successfully")

        conn.commit()
        print("PIPELINE PASSED!")

    except Exception as e:
        conn.rollback()
        conn.autocommit = True
        log_step(cur, "PIPELINE", "FAIL", str(e))
        print("PIPELINE FAILED:", e)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    setup_tables()
    validate_data()
