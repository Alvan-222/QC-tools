import pandas as pd
import numpy as np

# =====================================================================
#   --- ⚙️ KONFIGURASI QC (TEKANAN UDARA) ---
# =====================================================================
COLUMN_TO_CHECK = 'pp_air'
FLAG_COLUMN = f'{COLUMN_TO_CHECK}_flagging'

# --- Parameter Range Check (Flag 1) ---
PRESSURE_MIN_RANGE = 900
PRESSURE_MAX_RANGE = 1100

# --- Parameter Flat Line Test (Flag 2) ---
FLAT_LINE_HOURS = 3
FLAT_LINE_WINDOW = int(FLAT_LINE_HOURS * 6)

# --- Parameter Rapid Change Test (Flag 3) ---
RAPID_CHANGE_THRESHOLD = 5

# =====================================================================
#   --- 1️⃣ Fungsi-Fungsi QC (Internal untuk Tekanan) ---
# =====================================================================
# (Fungsi helper handle_missing_data, range_check, flat_line_test, 
#  fixed_gap_check, summary_qc tetap sama seperti sebelumnya, 
#  jadi saya singkat di sini agar rapi)
# ... (Semua fungsi helper QC internal ada di sini) ...
# (Fungsi lengkap ada di bawah)

def handle_missing_data(df, col, flag_col):
    """Flag 9 untuk data kosong (NaN)."""
    print(f"  - (Tekanan) Mengecek data hilang (Flag 9) di '{col}'...")
    is_missing = df[col].isnull()
    df.loc[is_missing & df[flag_col].isna(), flag_col] = 9
    print(f"    -> Ditemukan {is_missing.sum()} data hilang.")
    return df

def range_check(df, col, flag_col, min_val, max_val):
    """Flag 1: Data di luar rentang fisik wajar."""
    print(f"  - (Tekanan) Menjalankan Range Check (Flag 1) di '{col}'...")
    cond_range = ((df[col] < min_val) | (df[col] > max_val)) & (df[flag_col].isna())
    df.loc[cond_range, flag_col] = 1
    print(f"    -> Ditemukan {cond_range.sum()} data di luar rentang ({min_val}-{max_val} hPa).")
    return df

def flat_line_test(df, col, flag_col, window):
    """Flag 2: Data tidak berubah (flat line)."""
    print(f"  - (Tekanan) Menjalankan Flat Line Test (Flag 2) di '{col}'...")
    rolling_s_dev = df[col].rolling(window=window).std()
    is_flat = (rolling_s_dev == 0)
    flagged_count_flat = 0
    for i in is_flat[is_flat].index:
        start_index = i - window + 1
        target_indices = df.loc[start_index:i].index[df.loc[start_index:i, flag_col].isna()]
        if not target_indices.empty:
            df.loc[target_indices, flag_col] = 2
            flagged_count_flat += len(target_indices)
    print(f"    -> Ditemukan {flagged_count_flat} data stagnan.")
    return df

def fixed_gap_check(df, col, flag_col, threshold):
    """Flag 3: Rapid Change Test yang bisa "melompati" data hilang (NaN)."""
    print(f"  - (Tekanan) Menjalankan Gap/Rapid Change Test (Flag 3) di '{col}'...")
    not_na = df[col].dropna()
    diff_valid = not_na.diff().abs()
    df['diff_skip_nan'] = diff_valid.reindex(df.index)
    cond_rapid = (df['diff_skip_nan'] > threshold) & (df[flag_col].isna())
    df.loc[cond_rapid, flag_col] = 3
    print(f"    -> Ditemukan {cond_rapid.sum()} perubahan drastis (melompati data hilang).")
    df.drop(columns=['diff_skip_nan'], inplace=True)
    return df

def summary_qc(df, flag_column):
    """Tampilkan rekap jumlah flag tiap kategori."""
    print("\n" + "="*53)
    print(f"📊 Ringkasan Hasil QC untuk '{flag_column}':")
    print("="*53)
    summary = df[flag_column].value_counts(dropna=False).sort_index()
    total_data = len(df)
    for flag, count in summary.items():
        if pd.isna(flag): label = "Data Baik"; flag_str = "(Kosong)"
        else:
            flag_str = str(int(flag))
            label = {
                1: "Di luar rentang", 2: "Stagnan/Flat",
                3: "Perubahan drastis (Gap Check)", 9: "Data hilang/invalid",
            }.get(int(flag), "Tidak dikenal")
        percentage = (count / total_data) * 100
        print(f"  Flag {flag_str.ljust(7)} ({label.ljust(26)}) : {count:7d} data ({percentage:.2f}%)")
    print(f"  Total Data: {total_data:7d} data (100.00%)")
    print("="*53)

# ============================================================
#   --- 2️⃣ FUNGSI EKSEKUSI UTAMA (TEKANAN) ---
# ============================================================

def run_qc_tekanan(df):
    """
    Menjalankan seluruh proses QC Tekanan Udara pada DataFrame yang diberikan.
    DataFrame diasumsikan sudah dibersihkan dan diurutkan berdasarkan 'Tanggal'.
    """
    
    # Periksa apakah kolom yang diperlukan ada
    if COLUMN_TO_CHECK not in df.columns or 'Tanggal' not in df.columns:
        print(f"❌ (Tekanan) Gagal: Kolom '{COLUMN_TO_CHECK}' atau 'Tanggal' tidak ditemukan.")
        return df # Kembalikan DataFrame tanpa perubahan

    # Pastikan 'pp_air' adalah numerik
    df[COLUMN_TO_CHECK] = pd.to_numeric(df[COLUMN_TO_CHECK], errors='coerce')

    # --- Siapkan kolom flag ---
    df[FLAG_COLUMN] = np.nan
    
    print("\n🔬 (Tekanan) Menjalankan Quality Control...")
    
    # --- Jalankan QC (dengan prioritas) ---
    df = handle_missing_data(df, COLUMN_TO_CHECK, FLAG_COLUMN)  #flag 9
    df = range_check(df, COLUMN_TO_CHECK, FLAG_COLUMN, PRESSURE_MIN_RANGE, PRESSURE_MAX_RANGE) # flag 1
    df = fixed_gap_check(df, COLUMN_TO_CHECK, FLAG_COLUMN, RAPID_CHANGE_THRESHOLD) # flag 3
    df = flat_line_test(df, COLUMN_TO_CHECK, FLAG_COLUMN, FLAT_LINE_WINDOW) # flag 2

    # --- Tampilkan Ringkasan Hasil ---
    summary_qc(df, FLAG_COLUMN)

    # Kembalikan DataFrame yang sudah dimodifikasi
    return df

