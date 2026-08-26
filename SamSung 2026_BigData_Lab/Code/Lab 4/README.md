# BUỔI 04 — HADOOP HDFS VÀ THUẬT TOÁN MAPREDUCE

**60 phút** (Lý thuyết 25' + Thực hành 35') · Dataset: `ai4i2020.csv` — tiếp nối trực tiếp Buổi 01

## Tệp trong thư mục này

| Tệp | Dành cho | Nội dung |
|:---|:---|:---|
| `Buoi_04_Huong_Dan_Thuc_Hanh.docx` | Giảng viên + Học viên | Giáo án: lý thuyết HDFS/MapReduce, 6 bước thực hành, bảng lệnh CLI, xử lý lỗi, 2 phụ lục |
| `Buoi_04_Hadoop_Student.ipynb` | Học viên | Notebook 29 ô, có prompt copy-được, gợi ý giải, và **6 ô tự chấm điểm** |
| `Buoi_04_Huong_Dan_Thuc_Hanh.md` | — | Bản nguồn Markdown của file .docx |

## Bắt đầu

```bash
cd ../../00_Huong_Dan_Chung_va_Docker && docker compose up -d hadoop-namenode hadoop-datanode
```

Đợi 30–40 giây cho cả hai container đạt trạng thái `healthy`, rồi mở `Buoi_04_Hadoop_Student.ipynb` và chạy ô thiết lập đầu tiên.

Giao diện web NameNode: **http://localhost:9870**

## Sáu bước thực hành

| Bước | Thời lượng | Nội dung |
|:---|:---|:---|
| **D1** | 5' | Kiểm tra sức khỏe cụm, đọc `dfsadmin -report`, mở web UI |
| **D2** | 6' | Đưa `ai4i2020.csv` lên HDFS (quy trình 2 bước: `docker cp` → `hdfs dfs -put`) |
| **D3** | 4' | Khám phá cơ chế chia khối bằng `fsck` — tạo file 4,9 MB, khối 1 MB → **5 khối** |
| **D4** | 8' | Mô phỏng MapReduce bằng Python, thấy rõ 4 giai đoạn: 10.000 cặp → 3 khóa → 3 dòng |
| **D5** | 8' | Chạy job MapReduce **thật** bằng Hadoop Streaming + `awk` |
| **D6** | 4' | Đối chiếu với Pandas, thảo luận "vì sao còn cần Hadoop?" |

## Điểm nhấn sư phạm

**Kết quả MapReduce phải trùng khớp Buổi 01.** Cuối buổi, job Hadoop cho ra đúng ba con số mà `df.groupby("Type")` đã cho: **L 3,92% · M 2,77% · H 2,09%**. Khi hai công nghệ hoàn toàn khác nhau hội tụ về cùng một kết quả, học viên hiểu MapReduce không phải phép thuật — nó là `groupby` viết lại để chạy trên nhiều máy.

## Hai điều giảng viên cần biết trước

1. **`docker-compose.yml` không có YARN.** Job MapReduce chạy bằng **LocalJobRunner** (`mapreduce.framework.name` mặc định đã là `local` trong cụm này). HDFS, bộ đếm job, `_SUCCESS`, `part-00000` đều **thật hoàn toàn**; chỉ phần phân tán qua nhiều máy vật lý là không có. Phụ lục A trong file .docx cung cấp cấu hình YARN đầy đủ nếu cần.

2. **Container Hadoop không cài Python.** Mapper/Reducer viết bằng **`awk`** — có sẵn trong mọi bản Linux, và ngắn gọn hơn nên học viên nhìn ra bản chất nhanh hơn. Phụ lục B hướng dẫn cách cài Python nếu muốn.

## Sản phẩm học viên nộp

1. `Buoi_04_Hadoop_Student.ipynb` — đã chạy hết, không còn ô TODO trống
2. `outputs/report_buoi04.md` — báo cáo lệnh đã chạy và kết quả
3. Ảnh chụp `localhost:9870` — tab Overview và tab Browse the file system

Ô cuối notebook (**Bảng nghiệm thu**) tự chấm 8 mục trước khi nộp.
