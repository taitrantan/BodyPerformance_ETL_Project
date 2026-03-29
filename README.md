# Body Performance Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-green?logo=apache-airflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

## 🏗 Kiến trúc hệ thống

Hệ thống được thiết kế theo mô hình ETL Pipeline và được điều phối bởi Apache Airflow, chạy trong môi trường container hóa với Docker.

ETL này dùng để thu thập, xử lý và phân tích dữ liệu thể lực nhằm đánh giá và phân loại mức độ sức khỏe của con người.

1.  **Extract (E):** Tải dữ liệu từ file Excel. BodyPerformance.xlsx
2.  **Transform (T):** Kiểm tra, Làm sạch dữ liệu và tính toán chỉ số BMI
3.  **Load (L):** Lưu trữ dữ liệu vào **PostgreSQL Data Warehouse**.

## 📂 Cấu trúc dự án

```text
body_etl_project/
├── dags/
│   └── etl_pipeline.py        # Airflow DAG định nghĩa workflow 
├── scripts/
│   ├── load_raw.py            # Đọc Excel và load dữ liệu vào bản raw_body
│   ├── clean_data.py          # Làm sạch dữ liệu, tính BMI, phân loại BMI
│   └── load_dw.py             # Load dữ liệu đã xử lý sang Data Warehouse
│   └── etl_log.py             #Tạo log, validation
├── data/
│   └── BodyPerformance.xlsx   # File dữ liệu gốc 
├── docker-compose.yaml        # Cấu hình chạy Airflow bằng Docker
├── requirements.txt           # Danh sách thư viện Python (pandas, psycopg2,openpyxl...)
└── README.md                  # Tài liệu mô tả project
```

## 🚀 Hướng dẫn khởi chạy
1. **Cài đặt Docker & Docker-Compose:**  
   Đảm bảo máy bạn đã cài đặt Docker và Docker-Compose.
2. **Clone repository hoặc tải mã nguồn về máy**  

3. **Chạy lệnh:**
    ```bash
    docker-compose up -d
    ```
4. **Truy cập giao diện Airflow:**
    Mở trình duyệt và truy cập `http://localhost:8080`  
    Đăng nhập với tài khoản mặc định: `admin` / `admin`

5. **Kích hoạt DAG:**  
    Trong giao diện Airflow, tìm DAG có tên `Body_Performance_Pipeline` và bật nút **Toggle** để kích hoạt.

## 📋 Yêu cầu hệ thống

### Phần mềm cần thiết
- **Docker Desktop** (Windows/Mac) hoặc **Docker Engine** (Linux) - Version 20.10+
- **Docker Compose** - Version 2.0+
- **Git** - Để clone repository
- **4GB RAM** trở lên (khuyến nghị 8GB cho môi trường development)
- **20GB** dung lượng đĩa trống

### Hệ điều hành được hỗ trợ
- ✅ Windows 10/11 (với WSL2)
- ✅ macOS 11.0+
- ✅ Linux (Ubuntu 20.04+, Debian, CentOS, etc.)


## Công thức tính chỉ số BMI

Chỉ số BMI (Body Mass Index) được tính theo công thức:
𝐵𝑀𝐼=𝑤𝑒𝑖𝑔ℎ𝑡(𝑘𝑔)/(ℎ𝑒𝑖ght(𝑚)^2)

## Trong đó:
weight (kg): cân nặng (kilogram)
height (m): chiều cao (mét)

Chỉ số khối cơ thể (BMI) theo Tổ chức Y tế Thế giới (WHO)

| BMI Score | BMI Group |
|------------|--------------|
| <18.5 | Underweight |
| 18.5-25 | Normal |
| 25-30 | Overweight |
| 30-34.9 | Obese |
| >35 | Extremely Obese |

## 📂 Kết quả

Tại Postgres, trong database Body_Performance sẽ xuất hiện 2 bảng:
raw_body
dw_body

### Bảng raw_body
1. Tạo thêm cột ID, bmi, bmi_group. 
2. Từng phần tử được đánh ID, tính chỉ số BMI, xếp loại nhóm BMI.

### Bảng dw_body
1. Bảng dw_body thể hiện dữ liệu đã được tổng hợp để phân tích sức khỏe theo từng nhóm người.
2. Bao gồm các cột:   gender,
    class,
    AVG(age),
    AVG(bmi),
    AVG(gripForce),
    AVG(sit_ups)
3. Data thể hiện nhóm khỏe nhất, nam và nữ, thể lực theo BMI.

## 📂 Logging
Log sẽ được lưu vào: 
log/
