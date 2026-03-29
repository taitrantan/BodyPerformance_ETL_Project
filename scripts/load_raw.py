import pandas as pd
import psycopg2
import sys

def load_raw():
    conn = None
    cur = None
    try:
        
        print("Start loading raw data...")

        df = pd.read_excel('/opt/airflow/data/BodyPerformance.xlsx')

        # 2. Rename cột cho dễ xử lý 
        df.columns = [
            "age",
            "gender",
            "height_cm",
            "weight_kg",
            "body_fat",
            "diastolic",
            "systolic",
            "gripForce",
            "flexibility",
            "sit_ups",
            "broad_jump",
            "class"
        ]

        # 3. Kết nối DB
        conn = psycopg2.connect(
            host="host.docker.internal",   # nếu chạy docker
            database="Body_Performance",
            user="etl_worker",
            password="MindX15",
            port=5432
        )

        cur = conn.cursor()
        conn.autocommit = True

        # 4. Tạo bảng raw nếu chưa có
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_body (
                id SERIAL PRIMARY KEY,
                age INT,
                gender TEXT,
                height_cm FLOAT,
                weight_kg FLOAT,
                body_fat FLOAT,
                diastolic INT,
                systolic INT,
                gripForce FLOAT,
                flexibility FLOAT,
                sit_ups INT,
                broad_jump FLOAT,
                class TEXT
            );
        """)

        # 5. Xóa data cũ (tránh trùng)
        cur.execute("DELETE FROM raw_body;")

        # 6. Insert dữ liệu
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO raw_body (
                    age, gender, height_cm, weight_kg, body_fat,
                    diastolic, systolic, gripForce, flexibility,
                    sit_ups, broad_jump, class
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

        conn.commit()
        # cur.close()
        # conn.close()
        print("Load raw data done!")      

    except Exception as e:
        print("Error loading raw data:", e)
        if conn:
            conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    load_raw()