from common.get_spark import create_spark_session
from common.trino_connection import trino_query
from prefect import task
# Tạo table ở bronze

BRONZE= """
CREATE TABLE IF NOT EXISTS iceberg.bronze.giao_dich
(
    ma_giao_dich BIGINT,
    ma_khach_hang BIGINT,
    ten VARCHAR,
    email VARCHAR,
    so_dien_thoai VARCHAR,
    dia_chi VARCHAR,
    thanh_pho VARCHAR,
    zipcode VARCHAR,
    quoc_gia VARCHAR,
    tuoi BIGINT,
    gioi_tinh VARCHAR,
    phan_khuc_khach_hang VARCHAR,
    ngay VARCHAR,
    thoi_gian VARCHAR,
    so_luong BIGINT,
    gia DOUBLE,
    tong_tien DOUBLE,
    loai_san_pham VARCHAR,
    thuong_hieu VARCHAR,
    phan_hoi VARCHAR,
    phuong_thuc_giao_hang VARCHAR,
    phuong_thuc_thanh_toan VARCHAR,
    trang_thai_don_hang VARCHAR,
    danh_gia BIGINT,
    san_pham VARCHAR,
    ma_san_pham VARCHAR

)
"""
SILVER= """
CREATE TABLE IF NOT EXISTS iceberg.silver.giao_dich
(
    ma_giao_dich BIGINT,
    ma_khach_hang BIGINT,
    ten VARCHAR,
    email VARCHAR,
    so_dien_thoai VARCHAR,
    dia_chi VARCHAR,
    thanh_pho VARCHAR,
    zipcode VARCHAR,
    quoc_gia VARCHAR,
    tuoi BIGINT,
    gioi_tinh VARCHAR,
    phan_khuc_khach_hang VARCHAR,
    ngay VARCHAR,
    thoi_gian VARCHAR,
    so_luong BIGINT,
    gia DOUBLE,
    tong_tien DOUBLE,
    loai_san_pham VARCHAR,
    thuong_hieu VARCHAR,
    phan_hoi VARCHAR,
    phuong_thuc_giao_hang VARCHAR,
    phuong_thuc_thanh_toan VARCHAR,
    trang_thai_don_hang VARCHAR,
    danh_gia BIGINT,
    san_pham VARCHAR,
    ma_san_pham VARCHAR

)
"""
GOLD1= """
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_giao_dich (
    ma_giao_dich BIGINT,
    ma_khach_hang BIGINT,
    ma_san_pham VARCHAR,
    ngay VARCHAR,
    thoi_gian VARCHAR,
    phuong_thuc_giao_hang VARCHAR,
    phuong_thuc_thanh_toan VARCHAR,
    trang_thai_don_hang VARCHAR,
    phan_hoi VARCHAR,
    so_luong BIGINT,
    gia DOUBLE,
    tong_tien DOUBLE,
    danh_gia BIGINT
)
"""
GOLD2= """
    CREATE TABLE IF NOT EXISTS iceberg.gold.dim_khach_hang (
    ma_khach_hang BIGINT,
    ten VARCHAR,
    email VARCHAR,
    so_dien_thoai VARCHAR,
    dia_chi VARCHAR,
    thanh_pho VARCHAR,
    zipcode VARCHAR,
    quoc_gia VARCHAR,
    tuoi BIGINT,
    gioi_tinh VARCHAR,
    phan_khuc_khach_hang VARCHAR
)
"""
GOLD3= """
    CREATE TABLE IF NOT EXISTS iceberg.gold.dim_san_pham (
    ma_san_pham VARCHAR,
    loai_san_pham VARCHAR,
    thuong_hieu VARCHAR,
    san_pham VARCHAR
)

"""

def define_tables():
    trino_query(BRONZE)
    trino_query(SILVER)
    trino_query(GOLD1)
    trino_query(GOLD2)
    trino_query(GOLD3)
    print("==== Đã tạo các bảng trong Iceberg thành công ====")

define_tables()