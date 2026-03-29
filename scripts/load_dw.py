import psycopg2


def load_dw():
    try:
        print("Start loading Data Warehouse...")

        conn = psycopg2.connect(
            host="host.docker.internal",   # nếu chạy docker
            database="Body_Performance",
            user="etl_worker",
            password="MindX15",
            port=5432
        )

        cur = conn.cursor()

        # 1. Tạo bảng DW
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dw_body (               
                gender TEXT,
                class TEXT,
                avg_age FLOAT,
                avg_bmi FLOAT,
                avg_grip_force FLOAT,
                avg_situps FLOAT
            );
        """)

        # 2. Xóa dữ liệu cũ (tránh trùng khi chạy lại DAG)
        cur.execute("DELETE FROM dw_body;")

        # 3. Insert dữ liệu đã clean (aggregate)
        cur.execute("""
            INSERT INTO dw_body (
                gender, class, avg_age, avg_bmi, avg_grip_force, avg_situps
            )
            SELECT
                gender,
                class,
                AVG(age),
                AVG(bmi),
                AVG(gripForce),
                AVG(sit_ups)
            FROM raw_body
            GROUP BY gender, class;
        """)

        conn.commit()

    except Exception as e:
        print("Error loading Data Warehouse:", e)
        conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    print("Load Data Warehouse done!")

if __name__ == "__main__":
    load_dw()