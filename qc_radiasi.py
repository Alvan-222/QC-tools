import pandas as pd
import numpy as np

# =====================================================================
#   --- ⚙️ KONFIGURASI QC (RADIASI MATAHARI) ---
# =====================================================================
COLUMN_TO_CHECK = 'sr_avg'
FLAG_COLUMN = f'{COLUMN_TO_CHECK}_flagging'

# --- Parameter Range Check (Flag 1) ---
SR_MIN_RANGE = 0      
SR_MAX_RANGE = 1500    

# --- Parameter Flat Line Test (Flag 2) ---
FLAT_LINE_HOURS = 3.0
FLAT_LINE_WINDOW = int(FLAT_LINE_HOURS * 6) 
FLAT_LINE_STD_THRESH = 0.1      
FLAT_LINE_MIN_VALUE = 20        

# --- Parameter Rapid Change & Spike Test (Flag 3 & 4) ---
RAPID_CHANGE_THRESHOLD = 900.0  

# =====================================================================
#   --- 1️⃣ Fungsi-Fungsi QC (Internal untuk Radiasi) ---
# =====================================================================

def handle_missing_data(df, col, flag_col):
    """Flag 9: Data kosong (NaN)."""
    print(f"  - (Radiasi) Mengecek data hilang (Flag 9) di '{col}'...")
    cond = df[col].isna()
    df.loc[cond & df[flag_col].isna(), flag_col] = 9
    print(f"    -> Ditemukan {cond.sum()} data hilang.")
    return df

def range_check(df, col, flag_col, min_val, max_val):
    """Flag 1: Nilai di luar rentang wajar, KECUALI untestable."""
    print(f"  - (Radiasi) Menjalankan Range Check (Flag 1) di '{col}'...")
    is_exempt = df['is_untestable']
    cond_range = (((df[col] < min_val) | (df[col] > max_val)) & (df[flag_col].isna()) & (~is_exempt))
    df.loc[cond_range, flag_col] = 1
    print(f"    -> {cond_range.sum()} data (bisa dites) di luar rentang ({min_val} s/d {max_val} W/m²).")
    return df

def flat_line_test(df, col, flag_col, window, min_value, std_thresh):
    """Flag 2: Data stagnan saat siang, KECUALI untestable."""
    print(f"  - (Radiasi) Menjalankan Flat Line Test (Flag 2) di '{col}'...")
    rolling_std = df[col].rolling(window=window).std()
    cond = ((rolling_std <= std_thresh) & (df[col] > min_value) & (df['is_untestable'] == False))
    flagged_count = 0
    for i in cond[cond].index:
        start_index = i - window + 1
        target_indices = df.loc[start_index:i].index[
            (df.loc[start_index:i, flag_col].isna()) & 
            (df.loc[start_index:i, 'is_untestable'] == False)
        ]
        if not target_indices.empty:
            df.loc[target_indices, flag_col] = 2
            flagged_count += len(target_indices)
    print(f"    -> {flagged_count} data stagnan (std <= {std_thresh} & val > {min_value}).")
    return df

def fixed_spike_test(df, col, flag_col, threshold):
    """Flag 4: Spike - Mengabaikan data 'untestable', 'change_unreliable', first_of_day, dan tetangga=9."""
    print(f"  - (Radiasi) Menjalankan Spike Test (Flag 4) di '{col}'...")
    df['diff_prev'] = df[col].diff()
    df['diff_next'] = df[col].diff(-1)
    df['flag_prev'] = df[flag_col].shift(1)
    df['flag_next'] = df[flag_col].shift(-1)
    is_exempt_for_change = df['is_first_of_day'] | df['is_untestable'] | df['is_change_unreliable']
    cond = (
        (df['diff_prev'].abs() > threshold) & (df['diff_next'].abs() > threshold) &
        (df['diff_prev'] * df['diff_next'] < 0) & (df[flag_col].isna()) & 
        (df['flag_prev'] != 9) & (df['flag_next'] != 9) & (~is_exempt_for_change)
    )
    df.loc[cond, flag_col] = 4
    print(f"    -> {cond.sum()} spike terdeteksi (> {threshold} W/m²).")
    df.drop(columns=['diff_prev', 'diff_next', 'flag_prev', 'flag_next'], inplace=True, errors='ignore')
    return df

def fixed_rapid_change_test(df, col, flag_col, threshold):
    """Flag 3: Rapid Change - Mengabaikan data 'untestable', 'change_unreliable', first_of_day, dan tetangga=9."""
    print(f"  - (Radiasi) Menjalankan Rapid Change Test (Flag 3) di '{col}'...")
    df['diff_prev'] = df[col].diff().abs()
    df['flag_prev'] = df[flag_col].shift(1)
    is_exempt_for_change = df['is_first_of_day'] | df['is_untestable'] | df['is_change_unreliable']
    cond = (
        (df['diff_prev'] > threshold) & (df[flag_col].isna()) & 
        (df['flag_prev'] != 9) & (~is_exempt_for_change)
    )
    df.loc[cond, flag_col] = 3
    print(f"    -> {cond.sum()} perubahan drastis terdeteksi (> {threshold} W/m²).")
    df.drop(columns=['diff_prev', 'flag_prev'], inplace=True, errors='ignore')
    return df

def summary_qc(df, flag_column):
    """Tampilkan rekap jumlah flag tiap kategori."""
    print("\n" + "=" * 55)
    print(f"📊 Ringkasan QC untuk '{flag_column}'")
    print("=" * 55)
    summary = df[flag_column].value_counts(dropna=False).sort_index()
    total = len(df)
    for flag, count in summary.items():
        if pd.isna(flag): label = "Data Baik"; fcode = "(kosong)"
        else:
            label = {
                1: "Di luar rentang", 2: "Stagnan (Siang)", 3: "Perubahan Drastis",
                4: "Spike (Lonjakan)", 9: "Data Hilang"
            }.get(int(flag), "Tidak dikenal"); fcode = str(int(flag))
        print(f"  Flag {fcode:<7} {label:<20}: {count:6d} data ({count/total*100:.2f}%)")
    print(f"  Total Data: {total} (100%)")
    print("=" * 55)

# =====================================================================
#   🚀 2️⃣ FUNGSI EKSEKUSI UTAMA (RADIASI)
# =====================================================================

def run_qc_radiasi(df):
    """
    Menjalankan seluruh proses QC Radiasi Matahari pada DataFrame yang diberikan.
    DataFrame diasumsikan sudah dibersihkan dan diurutkan berdasarkan 'Tanggal'.
    """
    
    # Periksa apakah kolom yang diperlukan ada
    if COLUMN_TO_CHECK not in df.columns or 'Tanggal' not in df.columns:
        print(f"❌ (Radiasi) Gagal: Kolom '{COLUMN_TO_CHECK}' atau 'Tanggal' tidak ditemukan.")
        return df 

    # Pastikan 'sr_avg' adalah numerik
    df[COLUMN_TO_CHECK] = pd.to_numeric(df[COLUMN_TO_CHECK], errors='coerce')
    
    # --- Siapkan kolom flag ---
    df[FLAG_COLUMN] = np.nan
    
    # --- Tambahkan helper columns (khusus radiasi) ---
    print("🔄 (Radiasi) Mempersiapkan data...")
    df['is_first_of_day'] = ~df['Tanggal'].dt.floor('D').duplicated(keep='first')
    df['is_untestable'] = df[COLUMN_TO_CHECK].shift(1).isnull() & (~df['is_first_of_day'])
    df['is_change_unreliable'] = df['is_untestable'].shift(1).fillna(False)

    print("\n🔬 (Radiasi) Menjalankan Quality Control...")

    # --- Jalankan QC (dengan prioritas) ---
    df = handle_missing_data(df, COLUMN_TO_CHECK, FLAG_COLUMN) # flag 9
    df = range_check(df, COLUMN_TO_CHECK, FLAG_COLUMN, SR_MIN_RANGE, SR_MAX_RANGE) # flag 1
    df = fixed_spike_test(df, COLUMN_TO_CHECK, FLAG_COLUMN, RAPID_CHANGE_THRESHOLD) # flag 4
    df = fixed_rapid_change_test(df, COLUMN_TO_CHECK, FLAG_COLUMN, RAPID_CHANGE_THRESHOLD) # flag 3
    df = flat_line_test(df, COLUMN_TO_CHECK, FLAG_COLUMN, 
                         FLAT_LINE_WINDOW, FLAT_LINE_MIN_VALUE, FLAT_LINE_STD_THRESH) # flag 2

    # Hapus kolom helper
    kolom_dihapus = ['is_untestable', 'is_change_unreliable', 'is_first_of_day']
    df.drop(columns=kolom_dihapus, inplace=True, errors='ignore')
    print(f"\nKolom internal telah dihapus.")
    
    # --- Ringkasan hasil ---
    summary_qc(df, FLAG_COLUMN)

    # Kembalikan DataFrame yang sudah dimodifikasi
    return df

