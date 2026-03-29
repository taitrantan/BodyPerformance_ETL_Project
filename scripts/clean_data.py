import psycopg2

def clean_data():
    try:
        print("Start cleaning data...")

        conn = psycopg2.connect(
            host="host.docker.internal",   # nếu chạy docker
            database="Body_Performance",
            user="etl_worker",
            password="MindX15",
            port=5432
        )

        cur = conn.cursor()

        # 1. Xóa dữ liệu NULL quan trọng
        cur.execute("""
            DELETE FROM raw_body
            WHERE age IS NULL
            OR height_cm IS NULL
            OR weight_kg IS NULL;
        """)

        # 2. Xóa dữ liệu vô lý
        cur.execute("""
            DELETE FROM raw_body
            WHERE age <= 0
            OR height_cm < 100
            OR weight_kg <= 0;
        """)

        # 3. Chuẩn hóa gender (Male/Female → male/female)
        cur.execute("""
            UPDATE raw_body
            SET gender = LOWER(gender);
        """)

        # 4. Thêm cột BMI nếu chưa có
        cur.execute("""
            ALTER TABLE raw_body
            ADD COLUMN IF NOT EXISTS bmi FLOAT;
        """)

        # 5. Tính BMI = weight (kg) / (height (m))^2
        cur.execute("""
            UPDATE raw_body
            SET bmi = weight_kg / POWER(height_cm / 100, 2);
        """)

        # 6. Thêm cột nhóm BMI
        cur.execute("""
            ALTER TABLE raw_body
            ADD COLUMN IF NOT EXISTS bmi_group TEXT;
        """)

        # 7. Phân loại BMI < 18.5: Underweight, 18.5 ≤ BMI < 24.9: Normal,  25≤ BMI ≤ 29.9: Overweight, 30 ≤ BMI < 34.9: Obese, BMI ≥ 35: Extremely Obese
        cur.execute("""
            UPDATE raw_body
            SET bmi_group = CASE
                WHEN bmi < 18.5 THEN 'Underweight' 
                WHEN bmi >= 18.5 AND bmi < 25 THEN 'Normal'
                WHEN bmi >= 25 AND bmi < 30 THEN 'Overweight'
                WHEN bmi >= 30 AND bmi < 34.9 THEN 'Obese'
                ELSE 'Extremely Obese'
            END;
        """)

        conn.commit()
        
    except Exception as e:
        print("Error cleaning data:", e)
        conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    print("Clean data done!")

if __name__ == "__main__":
    clean_data()