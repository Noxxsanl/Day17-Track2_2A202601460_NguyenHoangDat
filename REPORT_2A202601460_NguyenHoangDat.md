# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Hoàng Đạt  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 32.4s
  run 2/3 … 32.0s
  run 3/3 … 32.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** — kèm cả hai bài mở rộng trong EXTRA.md (xem mục 4).

```
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố
  [consumer] đã ghi 20,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  B. chạy và bị giết ở lô 7
  [consumer] 💥 tiến trình bị giết ở lô 7
     -> tiến trình thoát với mã 137
     -> offset đã commit: 3,000

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 17,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✓
  C == A                            ✓
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: ĐẠT ✓
```

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau thao tác **Clear Task** trên Airflow (phiếu #1041), `gold_training_set` tăng số hàng sau mỗi lần chạy lại (38.750 → tăng thêm ở mỗi lượt), dù không có báo lỗi nào. |
| **Nguyên nhân** | `gold_training_set` là model `materialized = 'incremental'` nhưng không khai báo `unique_key`. Thiếu `unique_key`, dbt sinh câu lệnh **`INSERT`** đơn thuần cho mỗi lượt chạy thay vì upsert theo khoá — nên chạy lại cùng một khoảng dữ liệu là **ghi thêm** hàng cũ, không ghi đè. Vấn đề bị khuếch đại vì nguồn CDC (`bronze_tickets_cdc`) chứa bản ghi `op='u'` — cùng một `ticket_id` có thể xuất hiện nhiều lần trong lịch sử; do model chỉ lọc theo `_ingested_at` của lượt chạy (không phải theo khoá thực thể), mỗi lần Clear Task / chạy lại một ngày sẽ tái chèn toàn bộ các ticket của ngày đó chồng lên bản ghi đã có. Về phía DAG, `catchup=True` khiến Airflow tự lên lịch chạy bù tất cả các ngày quá khứ mỗi khi bị unpause/Clear, và việc thiếu `max_active_runs` cho phép nhiều run ghi đồng thời vào cùng bảng đích — hai tham số này không phải root cause nhưng làm **tăng tần suất** lỗi bị kích hoạt. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()` — vì grain của bảng là **entity** (1 hàng / 1 ticket) và `silver_tickets` đã dedup về đúng trạng thái mới nhất mỗi ticket, nên `merge` theo khoá tự nhiên `ticket_id` đảm bảo chạy lại sẽ upsert thay vì cộng dồn. `dags/ai_training_pipeline.py`: đặt `catchup=False` và `max_active_runs=1` để giảm khả năng kích hoạt lại lỗi tương tự trong tương lai. |
| **Bằng chứng** | trước: 38.750 hàng, ỔN ĐỊNH ✗ (checksum đổi mỗi lượt) · sau: **12.480 hàng** (khớp `expected/gold_training_set.count`), ỔN ĐỊNH ✓ — checksum `8dd7c98653` giống hệt ở cả 3 lượt chạy liên tiếp. Dòng `gold_training_set: 1 hàng / 1 ticket` chuyển từ ✗ (12.480 ticket bị lặp) sang ✓ (không lặp). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu ~5% hàng so với đối chiếu thủ công (8.645 / 9.100 kỳ vọng), và phần thiếu chỉ nằm ở những ngày đã chạy xong từ lâu — ngày mới nhất thì đủ. |
| **P99 độ trễ đo được** | **2,73 ngày** *(bắt buộc)* — đo bằng `quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99)` trên `bronze_events`. P50 = 0,13 ngày, P95 = 1,81 ngày, max = 2,94 ngày; ~5,05% bản ghi tới kho muộn hơn 1 ngày so với lúc sự kiện xảy ra. |
| **Lookback đã chọn** | 3 ngày — vì P99 ≈ 2,73 ngày, làm tròn lên 3 ngày để phủ gần hết phần đuôi phân bố mà vẫn giữ chi phí quét lại hợp lý ở mỗi lượt chạy sau. |
| **Nguyên nhân** | Khối `is_incremental()` lọc `where event_date > (select max(event_date) from {{ this }})` — nghĩa là chỉ xử lý những `event_date` **lớn hơn** giá trị lớn nhất đã có trong bảng đích, hoàn toàn không tính đến độ trễ giữa lúc sự kiện xảy ra (`event_time`) và lúc dữ liệu **tới kho** (`_ingested_at`). Một bản ghi có `event_date = 08-12` nhưng `_ingested_at = 08-15` (do đến muộn) sẽ có `max(event_date)` trong target đã vượt qua 08-12 từ lâu tại thời điểm 08-15 → bản ghi không bao giờ lọt qua điều kiện lọc, kể cả ở các lượt chạy sau. Đây là lỗi kinh điển khi xử lý **dữ liệu về muộn (late-arriving data)**: điều kiện incremental dùng mốc "ngày sự kiện" thay vì có một cửa sổ lùi lại (lookback window) đủ rộng để bắt lại phần dữ liệu đến sau. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện lọc thành `where event_date > (select max(event_date) from {{ this }}) - interval 3 day`, mở lookback window 3 ngày dựa trên P99 đo được. Vì window rộng hơn khiến cùng một cặp `(event_date, customer_id)` được tính lại ở nhiều lượt chạy, đã thêm `unique_key = ['event_date', 'customer_id']` (khoá kép — đúng grain 2 cột của bảng) và `incremental_strategy = 'merge'` để lần tính sau **thay thế** lần tính trước thay vì cộng dồn (tránh tái tạo lỗi của Nhiệm vụ 1). |
| **Bằng chứng** | trước: 8.645 hàng · sau: **9.100 hàng** (khớp `expected/gold_feature_daily.count`), ỔN ĐỊNH ✓ — checksum `3db448685c` giống hệt ở cả 3 lượt. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là một giá trị đơn lẻ, rất nhạy với outlier — chỉ cần một bản ghi đến muộn bất thường (do lỗi mạng, retry, v.v.) cũng có thể kéo `max` lên rất cao, buộc lookback phải mở rộng theo, và **mọi lượt chạy sau này** đều phải trả chi phí quét lại phần dữ liệu tương ứng với outlier đó, dù outlier chỉ xảy ra một lần. P99 là một phép đo phân vị, ổn định hơn nhiều trước nhiễu — nó phản ánh "hầu hết dữ liệu trễ đến đâu" thay vì "trường hợp tệ nhất từng xảy ra". Chọn lookback theo P99 (làm tròn lên) là đánh đổi có kiểm soát: chấp nhận bỏ sót một phần rất nhỏ (~1%) bản ghi cực trễ để giữ chi phí quét lại ổn định ở mọi lượt chạy, thay vì phải trả giá cho toàn bộ lịch sử mỗi khi có một outlier mới xuất hiện.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 08-10 (ngày team backend đổi kiểu cột `priority` từ số sang chuỗi), `silver_tickets.priority` có tỷ lệ NULL rất lớn (6.606 hàng sai), đồng thời còn tồn tại các giá trị ngoài miền hợp lệ (`0`, `5`, `-1`). Pipeline không dừng, nhưng model phân loại dự đoán kém hẳn kể từ mốc đó. |
| **Nguyên nhân** | Biểu thức chuẩn hoá gốc là `try_cast(priority_raw as integer)` — biểu thức này sai theo **hai hướng ngược nhau cùng lúc**: (1) nó biến toàn bộ nhãn chữ hợp lệ về mặt ngữ nghĩa (`urgent`, `high`, `medium`, `low` — do backend đổi cách biểu diễn kể từ 08-10, ý nghĩa dữ liệu không đổi) thành `NULL`, gây ra phần lớn 6.606 hàng NULL; (2) nó lại **chấp nhận** các giá trị số nằm ngoài contract (`0`, `5`, `-1`) vì chúng cast integer thành công, dù contract quy định `priority ∈ 1..4`. Gốc rễ là `try_cast` chỉ kiểm tra được "có phải số không", không kiểm tra được "có đúng ngữ nghĩa contract không", và không phân biệt được giữa **schema evolution hợp lệ** (đổi cách biểu diễn, giữ nguyên ý nghĩa) và **dữ liệu lỗi thật** (giá trị không mang thông tin hợp lệ của contract cũ dưới bất kỳ hình thức nào). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm 1 — số hợp lệ `1`/`2`/`3`/`4`: đúng contract ban đầu → giữ nguyên. Nhóm 2 — nhãn chữ `urgent`/`high`/`medium`/`low`: schema evolution, backend đổi cách ghi kể từ 08-10 nhưng ý nghĩa không đổi → map về số theo tài liệu API (`urgent→1, high→2, medium→3, low→4`). Nhóm 3 — giá trị rác `P1`/`unknown`/`0`/`5`/`-1`/`''`/`NULL`: không mang đúng thông tin của contract dưới bất kỳ cách biểu diễn nào → trả về `NULL`, đưa vào `quarantine_tickets`. |
| **Cách khắc phục** | (a) `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE` xử lý đủ 3 nhóm — số hợp lệ trong khoảng 1..4 giữ nguyên, nhãn chữ map về số tương ứng, còn lại trả `NULL`. Macro dùng chung cho cả `silver_tickets` và `quarantine_tickets` nên hai bảng không thể lệch nhau. (b) `dbt/models/silver/silver_tickets.sql`: lọc bỏ bản ghi có `normalize_priority(priority_raw) is null` **trước khi** xếp hạng bằng `row_number()` — nếu lọc sau xếp hạng, ticket nào có bản ghi mới nhất bị hỏng sẽ mất luôn cả ticket (12.480 → 12.168), trong khi đúng ra chỉ nên loại bản ghi hỏng, ticket vẫn giữ trạng thái hợp lệ từ lần cập nhật trước. (c) `dbt/models/silver/quarantine_tickets.sql`: đổi `where false` thành `where normalize_priority(priority_raw) is null`. (d) `dbt/models/silver/schema.yml`: bật `contract.enforced: true` và thêm test `not_null` + `accepted_values: [1,2,3,4]` cho cột `priority` — contract chỉ ràng buộc kiểu dữ liệu (integer), không ràng buộc miền giá trị, nên vẫn cần test riêng để chặn `priority = 99`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** (khớp `expected/quarantine_tickets.count`, đúng grain 1 hàng/1 bản ghi CDC lỗi) · `dbt test` **11/11 pass** (tăng từ 9 test gốc lên 11 nhờ 2 test mới trên `priority`) · `silver_tickets.priority` sạch, luôn ∈ 1..4, không còn NULL · `silver_tickets` vẫn giữ đủ **12.480** ticket. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn ở **Silver**, không phải Bronze. Bronze là bản sao thô, không suy diễn, của nguồn — nhiệm vụ của nó là bảo toàn nguyên trạng những gì hệ thống nguồn gửi tới, kể cả bản ghi "xấu". Nếu Bronze từ chối ngay các row có `priority_raw` lạ, đội vận hành sẽ mất khả năng điều tra sự cố về sau: không còn cách nào biết chính xác giá trị gốc mà source đã gửi, không phân biệt được đây là lỗi tạm thời của nguồn hay một thay đổi schema hợp lệ (như trường hợp `urgent/high/medium/low` ở nhiệm vụ này) — và quan trọng hơn, không có bằng chứng để đối chiếu khi làm việc lại với team backend. Việc phân loại "đây có phải lỗi thật không" đòi hỏi ngữ cảnh về contract và logic nghiệp vụ, đó là việc của tầng Silver, nơi dữ liệu bắt đầu được diễn giải theo ý nghĩa.
>
> Không nên để `dbt test` fail và dừng cả DAG khi gặp bản ghi lỗi, vì quy mô của vấn đề không tương xứng với thiệt hại của việc dừng toàn bộ pipeline: chỉ 312 bản ghi CDC lỗi trên tổng số hơn 14.300 bản ghi CDC, hơn 130.000 event và 31.200 chunk tài liệu — nếu để một tỷ lệ lỗi nhỏ như vậy chặn đứng cả DAG, thì RAG index, tập huấn luyện và bảng đặc trưng routing đều bị trì hoãn theo, ảnh hưởng tới toàn bộ hệ thống hỗ trợ khách hàng chỉ vì một phần rất nhỏ dữ liệu không hợp lệ. Tách riêng bản ghi lỗi vào `quarantine_tickets` cho phép pipeline tiếp tục phục vụ phần dữ liệu hợp lệ (chiếm đa số áp đảo) trong khi đội vận hành xử lý phần lỗi song song, không đồng bộ với luồng chính.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | `queries/dashboard.sql` mất 38 giây thay vì 2 giây như 3 tháng trước, dù không ai sửa dòng code nào. |
| **Nguyên nhân** | `data/gold_events/` gồm 5.000 file Parquet nhỏ, không partition, thứ tự hàng ngẫu nhiên — small-file problem: DuckDB đọc Parquet theo lô và làm tròn lên theo từng file, nên 5.000 file vài chục hàng vẫn tốn khối lượng đọc tương đương ~5.000.000 hàng dù dữ liệu thật chỉ có 130.683 hàng. Ngoài ra filter trong query là `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` — cột bị bọc trong function nên **không sargable**: engine không so được kết quả hàm với tên thư mục hay min/max statistics, buộc phải mở toàn bộ 5.000 file để biết file nào có ích. |
| **Cách khắc phục** | `tools/compact.py`: gộp 5.000 file thành dataset mới `data/gold_events_v2/`, `COPY ... TO ... PARTITION_BY (event_date)` — chọn `event_date` (14 giá trị phân biệt, khớp điều kiện lọc theo ngày) chứ không phải `customer_name` (650 giá trị — partition theo cột này sẽ tái tạo đúng small-file problem). Trong mỗi partition, `ORDER BY event_date, customer_name` để các hàng cùng khách hàng nằm liền nhau, giúp min/max của row group có ý nghĩa lọc theo `customer_name`. `ROW_GROUP_SIZE = 2000` (nhỏ hơn ~9.334 hàng/ngày) để mỗi ngày được chia thành nhiều row group thay vì gộp cả ngày vào một row group duy nhất (nếu vậy min/max sẽ phủ toàn bộ 650 khách hàng, mất tác dụng lọc). `queries/dashboard.sql`: trỏ vào dataset mới với `hive_partitioning = true`, viết lại filter ngày thành `event_date = DATE '2026-08-09'` (cột đứng riêng một vế, sargable). |
| **Bằng chứng** | `rows scanned`: 5.000.000 → **9.324** (giảm **536,3×**, vượt yêu cầu ≥10×) · `files`: 5.000 → **14** · `result hash`: `4379e4c5d9f3` không đổi ở cả hai lần đo — ngữ nghĩa truy vấn giữ nguyên. |

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test` kill tiến trình consumer giữa batch rồi khởi động lại; cần xác định consumer đang mất hay trùng dữ liệu. |
| **Nguyên nhân** | Thứ tự thao tác gốc trong `consume()` là `commit() → maybe_crash() → write_batch()`: offset được ghi nhận **trước khi** dữ liệu thực sự được ghi xuống kho. Nếu tiến trình chết ngay tại `maybe_crash()`, offset đã dịch nhưng batch chưa từng được ghi — lần khởi động lại đọc tiếp từ offset mới, batch đó vĩnh viễn không được xử lý. Đây là ngữ nghĩa **at-most-once**, đánh đổi lấy khả năng **mất dữ liệu** khi crash. |
| **Cách khắc phục** | `ingest/consumer.py`: đổi thứ tự thành `write_batch() → maybe_crash() → commit()` — chuyển sang ngữ nghĩa **at-least-once**: nếu crash xảy ra sau khi ghi nhưng trước khi commit, offset chưa dịch nên lần khởi động lại sẽ đọc lại (phát lại) đúng batch đó, đổi lấy khả năng **trùng dữ liệu** thay vì mất. Vì exactly-once không tồn tại ở tầng giao vận, phần còn lại là làm cho việc ghi **idempotent**: thêm ràng buộc `PRIMARY KEY (event_id)` vào DDL, và đổi `write_batch()` sang `INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...` — chọn `DO UPDATE` thay vì `DO NOTHING` vì một message phát lại luôn mang nội dung mới nhất mà nguồn đã gửi cho `event_id` đó; `DO NOTHING` sẽ âm thầm giữ lại bản ghi cũ hơn nếu nội dung đã đổi giữa hai lần gửi, trong khi `DO UPDATE` đảm bảo kho luôn phản ánh đúng lần ghi gần nhất. |
| **Bằng chứng** | `make crash-test`: chạy 20.000 message một mạch → 20.000 hàng/20.000 `event_id` khác nhau. Bị giết ở lô 7 (offset committed: 3.000), khởi động lại ghi nốt 17.000 message → vẫn đúng **20.000 hàng / 20.000 `event_id` khác nhau**, kết quả sau crash giống hệt kết quả không crash (`C == A`). `BÀI MỞ RỘNG B: ĐẠT ✓`. `make verify` sau đó vẫn giữ nguyên **4/4 tiêu chí** — ba nhiệm vụ chính không bị ảnh hưởng. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Với mọi model `incremental`, kiểm tra ngay `unique_key` và `incremental_strategy` trong `config()` — thiếu `unique_key` là dấu hiệu rõ nhất của một model sẽ nhân bản dữ liệu khi chạy lại, đặc biệt nguy hiểm với nguồn CDC có bản ghi `update`. |
| 2 | Đo phân bố độ trễ (`_ingested_at - event_time`) của mọi nguồn dữ liệu streaming/event trước khi tin vào điều kiện lọc incremental theo mốc thời gian — một điều kiện `WHERE cot_thoi_gian > max(...)` không có lookback gần như luôn đánh rơi dữ liệu đến muộn một cách âm thầm, không báo lỗi. |
| 3 | Đối chiếu phân bố giá trị thực tế của một cột (`group by ... count(*)`) với domain mà contract/test khai báo, đặc biệt ở các cột do hệ thống upstream khác kiểm soát (source đổi định dạng mà không báo, hoặc báo qua kênh không tự động hoá được như Slack) — và luôn phân biệt rõ "đổi cách biểu diễn" với "dữ liệu lỗi thật" trước khi quyết định map hay quarantine.
