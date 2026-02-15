import os
import json
import boto3
from datetime import datetime
from botocore.config import Config
from supabase import create_client

# --- CẤU HÌNH ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
# [QUAN TRỌNG] Đã sửa tên biến theo yêu cầu của bạn
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

R2_ENDPOINT = os.environ.get("R2_ENDPOINT_URL")
R2_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET_NAME")

# Check key tồn tại
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(f"❌ LỖI: Thiếu biến môi trường. Kiểm tra lại SUPABASE_SERVICE_ROLE_KEY trong Secrets.")

# Kết nối Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Kết nối R2
s3 = boto3.client('s3', endpoint_url=R2_ENDPOINT,
                  aws_access_key_id=R2_KEY_ID, aws_secret_access_key=R2_SECRET,
                  config=Config(signature_version='s3v4'))

def main():
    print(">>> BẮT ĐẦU MIGRATION HISTORY (FIX KEY & LEGACY) <<<")

    # 1. Lấy toàn bộ dữ liệu từ bảng tournaments
    response = supabase.table("tournaments").select("*").neq('id', -1).execute()
    all_tournaments = response.data
    print(f"-> Tổng số bản ghi trong DB: {len(all_tournaments)}")

    history_map = {}
    count_legacy = 0
    count_standard = 0
    today_str = datetime.utcnow().strftime('%Y-%m-%d')

    for record in all_tournaments:
        try:
            data = record.get("data") or {}
            db_id = record.get("id")

            # --- LOGIC LỌC HISTORY (Dựa trên SQL đã check) ---
            is_history = False
            
            # Check 1: Label FINALIZED
            ai_pred = data.get("ai_prediction") or {}
            if ai_pred.get("status_label") == "FINALIZED":
                is_history = True
            
            # Check 2: Expired Date (Ngày kết thúc nhỏ hơn hôm nay)
            end_date = data.get("end")
            if not is_history and end_date and end_date < today_str:
                is_history = True

            # Nếu KHÔNG PHẢI History (tức là Active) -> Bỏ qua
            if not is_history:
                continue

            # --- XỬ LÝ KEY CHO DATA ---
            alpha_id = data.get("alphaId")
            
            # Case 1: Hàng chuẩn (Có AlphaID)
            if alpha_id:
                object_key = alpha_id
                count_standard += 1
            # Case 2: Hàng Legacy (Thiếu AlphaID) -> Tạo ID giả legacy_ID
            else:
                object_key = f"legacy_{db_id}"
                # Inject ID giả vào data để Frontend hiển thị được, không bị lỗi
                data["alphaId"] = object_key 
                count_legacy += 1

            # --- CHUẨN HÓA DATA ---
            if not data.get("ai_prediction"):
                data["ai_prediction"] = {}
            # Đảm bảo đóng dấu FINALIZED
            data["ai_prediction"]["status_label"] = "FINALIZED"
            
            # Đưa vào Map
            history_map[object_key] = data
        
        except Exception as e:
            print(f"❌ Lỗi record ID {record.get('id')}: {e}")

    total_migrated = count_standard + count_legacy
    print("------------------------------------------------")
    print(f"✅ KẾT QUẢ QUÉT:")
    print(f"   - Giải chuẩn (Có AlphaID): {count_standard}")
    print(f"   - Giải cũ (Legacy):        {count_legacy}")
    print(f"   => TỔNG CỘNG HISTORY:      {total_migrated}")

    # 3. Upload R2
    if total_migrated > 0:
        file_key = "finalized_history.json"
        print(f"-> Đang upload '{file_key}' lên R2...")
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=file_key,
            Body=json.dumps(history_map),
            ContentType='application/json'
        )
        print("🎉 UPLOAD THÀNH CÔNG! R2 ĐÃ CÓ DỮ LIỆU LỊCH SỬ.")
    else:
        print("⚠️ Không tìm thấy dữ liệu history nào.")

if __name__ == "__main__":
    main()
