import pandas as pd
import numpy as np

# =====================================================================
#   --- ⚙️ KONFIGURASI QC (HUJAN) ---
# =====================================================================
CUMULATIVE_COLUMN = 'rr'
ORIGINAL_CUMULATIVE_COLUMN = 'rr_original_internal'
INTERVAL_COLUMN = 'rr_interval_internal' 
FLAG_COLUMN = 'rr_flagging'

# --- Parameter Range Check (Flag 1) ---
RR_MIN_RANGE = 0
RR_MAX_RANGE = 40

# --- Parameter Flat Line Test (Flag 2) ---
FLAT_LINE_HOURS = 24
FLAT_LINE_WINDOW = int(FLAT_LINE_HOURS * 6)
FLAT_LINE_MIN_VALUE = 0.1

# --- Parameter Rapid Change (Flag 3) & Spike Test (Flag 4) ---
RAPID_CHANGE_THRESHOLD = 30.0
# --- Parameter Unexpected Drop Test (Flag 5) ---
UNEXPECTED_DROP_THRESHOLD = -1.0

# =====================================================================
#   --- 1️⃣ Fungsi-Fungsi QC (Internal untuk Hujan) ---
# =====================================================================

def handle_original_missing_data(df, original_col, flag_col):
    """Flag 9: Data kumulatif ASLI kosong (NaN)."""
    print(f"  - (Hujan) Mengecek data kumulatif asli hilang (Flag 9) di '{original_col}'...")
    cond = df[original_col].isna()
    count_before = df[flag_col].isna().sum()
    df.loc[cond & df[flag_col].isna(), flag_col] = 9
    count_after = df[flag_col].isna().sum()
    flagged_count = count_before - count_after
    print(f"    -> {flagged_count} data kumulatif asli hilang ditandai Flag 9.")
    return df

def range_check(df, col, flag_col, min_val, max_val):
    """Flag 1: Nilai interval di luar rentang wajar."""
    print(f"  - (Hujan) Menjalankan Range Check (Flag 1) di '{col}'...")
    cond_range = (
        ((df[col] < min_val) | (df[col] > max_val)) &
        (df[flag_col].isna())
    )
    count_flagged = cond_range.sum()
    if count_flagged > 0:
        df.loc[cond_range, flag_col] = 1
        count_max = ((df[col] > max_val) & (df[flag_col] == 1)).sum()
        count_min = ((df[col] < min_val) & (df[flag_col] == 1)).sum()
        print(f"    -> {count_max} data interval di atas batas maksimum ({max_val} mm) ditandai Flag 1.")
        print(f"    -> {count_min} data interval negatif ditandai Flag 1.")
    else:
        print("    -> Tidak ada data interval di luar rentang.")
    return df

def unexpected_drop_test(df, raw_diff_col, flag_col, threshold):
    """Flag 5: Penurunan nilai kumulatif (raw_diff negatif) yang tidak wajar."""
    print(f"  - (Hujan) Menjalankan Unexpected Drop Test (Flag 5)...")
    
    cond_drop = (
        (df[raw_diff_col] < threshold) &
        (~df['is_hardcoded_reset_time_internal']) & 
        (df[flag_col].isna())
    )
    count_flagged = cond_drop.sum()
    if count_flagged > 0:
        df.loc[cond_drop, flag_col] = 5
        print(f"    -> {count_flagged} penurunan tidak wajar terdeteksi (< {threshold} mm) ditandai Flag 5.")
    else:
        print("    -> Tidak ada penurunan tidak wajar.")
    return df


def flat_line_test(df, col, flag_col, window, min_value):
    """Flag 2: Data interval stagnan saat ada hujan (flat line)."""
    print(f"  - (Hujan) Menjalankan Flat Line Test (Flag 2) di '{col}'...")
    valid_data = df[col].where(df[flag_col].isna())
    rolling_std = valid_data.rolling(window=window, min_periods=window).std()
    cond_end_of_flat = (rolling_std.fillna(1) <= 1e-9) & (df[col] > min_value) & df[flag_col].isna()

    flagged_count = 0
    indices_to_flag = set()
    for i in cond_end_of_flat[cond_end_of_flat].index:
        start_index = max(0, i - window + 1)
        potential_indices = df.loc[start_index:i].index[df.loc[start_index:i, flag_col].isna()]
        if not potential_indices.empty:
             window_data = df.loc[potential_indices, col]
             if window_data.max() - window_data.min() <= 1e-9 :
                 indices_to_flag.update(potential_indices.tolist())

    if indices_to_flag:
        df.loc[list(indices_to_flag), flag_col] = 2
        flagged_count = len(indices_to_flag)
        print(f"    -> {flagged_count} data interval stagnan (hujan > {min_value} mm) ditandai Flag 2.")
    else:
        print("    -> Tidak ada data interval stagnan saat hujan.")
    return df

def fixed_rapid_change_test(df, col, flag_col, threshold):
    """Flag 3: Nilai Interval (Rapid Change) melebihi threshold."""
    print(f"  - (Hujan) Menjalankan Rapid Change Test (Flag 3) di '{col}'...")
    
    is_exempt_for_change = df['is_reset_event_internal'] | df['is_change_unreliable']
    cond = (
        (df[col].abs() > threshold) &
        (df[flag_col].isna()) &
        (~is_exempt_for_change)
    )
    count_flagged = cond.sum()
    if count_flagged > 0:
        df.loc[cond, flag_col] = 3
        print(f"    -> {count_flagged} nilai interval drastis terdeteksi (> {threshold} mm) ditandai Flag 3.")
    else:
        print("    -> Tidak ada nilai interval drastis terdeteksi.")
    return df

def fixed_spike_test(df, col, flag_col, threshold):
    """Flag 4: Spike (perubahan antar interval) - Mengabaikan reset event dan data kedua setelah missing."""
    print(f"  - (Hujan) Menjalankan Spike Test (Flag 4) di '{col}'...")
    valid_data = df[col].where(df[col].notna() & df[flag_col].isna())
    df['diff_prev_interval'] = valid_data.diff()
    df['diff_next_interval'] = valid_data.diff(-1)
    neighbor_prev_is_invalid = df[col].shift(1).isna() | (df[flag_col].shift(1) == 9)
    neighbor_next_is_invalid = df[col].shift(-1).isna() | (df[flag_col].shift(-1) == 9)
    is_exempt_for_change = df['is_reset_event_internal'] | df['is_change_unreliable']

    cond = (
        (df['diff_prev_interval'].abs() > threshold) &
        (df['diff_next_interval'].abs() > threshold) &
        (df['diff_prev_interval'] * df['diff_next_interval'] < 0) &
        (df[flag_col].isna()) &
        (~neighbor_prev_is_invalid) &
        (~neighbor_next_is_invalid) &
        (~is_exempt_for_change)
    )
    count_flagged = cond.sum()
    if count_flagged > 0:
        df.loc[cond, flag_col] = 4
        print(f"    -> {count_flagged} spike (perubahan antar interval > {threshold} mm) ditandai Flag 4.")
    else:
         print("    -> Tidak ada spike (perubahan antar interval) terdeteksi.")
    df.drop(columns=['diff_prev_interval', 'diff_next_interval'], inplace=True, errors='ignore')
    return df

def summary_qc(df, flag_column):
    """Tampilkan rekap jumlah flag tiap kategori."""
    print("\n" + "=" * 55)
    print(f"📊 Ringkasan QC untuk '{flag_column}'")
    print("=" * 55)
    summary = df[flag_column].value_counts(dropna=False).sort_index()
    total = len(df)
    flag_labels = {
        1: "Di luar rentang", 2: "Stagnan (Hujan)", 3: "Interval Drastis",
        4: "Spike (Perubahan)", 5: "Penurunan Tdk Wajar",
        9: "Data Asli Hilang"
    }
    all_possible_flags = sorted(flag_labels.keys())
    summary_dict = summary.to_dict()

    print(f"  {'Flag':<7} {'Keterangan':<20}: {'Jumlah':>7} {'Persentase':>10}")
    print("-" * 55)
    good_count = summary_dict.get(np.nan, 0)
    print(f"  {'':<7} {'Data Baik/Tidak Diuji':<20}: {good_count:>7d} ({good_count/total*100:>9.2f}%)")
    for flag_int in all_possible_flags:
        count = summary_dict.get(float(flag_int), 0)
        if count > 0:
            label = flag_labels.get(flag_int, "Tidak dikenal")
            fcode = str(flag_int)
            print(f"  {fcode:<7} {label:<20}: {count:>7d} ({count/total*100:>9.2f}%)")
    print("-" * 55)
    print(f"  {'Total':<7} {'':<20}: {total:>7d} ({100.0:>9.2f}%)")
    print("=" * 55)


# =====================================================================
#   🚀 2️⃣ FUNGSI EKSEKUSI UTAMA (HUJAN)
# =====================================================================

def run_qc_hujan(df):
    """
    Menjalankan seluruh proses QC Hujan pada DataFrame yang diberikan.
    DataFrame diasumsikan sudah dibersihkan dan diurutkan berdasarkan 'Tanggal'.
    """

    if CUMULATIVE_COLUMN not in df.columns or 'Tanggal' not in df.columns:
        print(f"❌ (Hujan) Gagal: Kolom '{CUMULATIVE_COLUMN}' atau 'Tanggal' tidak ditemukan.")
        return df

    print("🔄 (Hujan) Mempersiapkan data interval...")
    df_hujan = df.copy()
    df_hujan[ORIGINAL_CUMULATIVE_COLUMN] = df_hujan[CUMULATIVE_COLUMN]
    df_hujan[CUMULATIVE_COLUMN] = pd.to_numeric(df_hujan[CUMULATIVE_COLUMN], errors='coerce')

    # =================================================================
    #   --- Hitung curah hujan per 10 menit & Helper Columns ---
    # =================================================================
    df_hujan['raw_diff_internal'] = df_hujan[CUMULATIVE_COLUMN].diff()
    is_prev_missing = df_hujan[CUMULATIVE_COLUMN].shift(1).isnull()
    is_reset_detected = (df_hujan['raw_diff_internal'] < 0) 

    waktu = df_hujan['Tanggal'].dt.time
    reset_time_1 = pd.to_datetime('00:00:00').time()
    reset_time_2 = pd.to_datetime('00:10:00').time()
    reset_time_3 = pd.to_datetime('00:20:00').time()
    
    # --- LOGIKA HELPER ---
    # Pengecualian HANYA untuk jam 00:00/00:10/00:20 (untuk Flag 5)
    is_hardcoded_reset_time = (waktu == reset_time_1) | (waktu == reset_time_2) | (waktu == reset_time_3)
    df_hujan['is_hardcoded_reset_time_internal'] = is_hardcoded_reset_time
    # Pengecualian untuk SEMUA reset (untuk Flag 3 & 4)
    df_hujan['is_reset_event_internal'] = is_reset_detected | is_hardcoded_reset_time

    is_untestable_original = is_prev_missing
    df_hujan['is_change_unreliable'] = is_untestable_original.shift(1).fillna(False)

    # --- Preprocessing: Invalidasi Data Setelah Unexpected Drop ---
    cond_pre_drop = (
        (df_hujan['raw_diff_internal'] < UNEXPECTED_DROP_THRESHOLD) &
        (~df_hujan['is_hardcoded_reset_time_internal']) & # <-- Cek HANYA hardcoded time
        (~is_prev_missing)
    )
    # PERBAIKAN BUG: Gunakan shift(1) untuk invalidasi data SETELAH drop
    indices_to_invalidate = df_hujan.index[cond_pre_drop.shift(1).fillna(False)]
    
    count_invalidated = 0
    if not indices_to_invalidate.empty:
        df_hujan.loc[indices_to_invalidate, CUMULATIVE_COLUMN] = np.nan
        count_invalidated = len(indices_to_invalidate)
        print(f"    -> {count_invalidated} data kumulatif setelah unexpected drop diinvalidasi (diubah jadi NaN).")
        
        is_prev_missing = df_hujan[CUMULATIVE_COLUMN].shift(1).isnull()
        df_hujan['raw_diff_internal'] = df_hujan[CUMULATIVE_COLUMN].diff()
        is_reset_detected = (df_hujan['raw_diff_internal'] < 0)
        df_hujan['is_reset_event_internal'] = is_reset_detected | is_hardcoded_reset_time
        is_untestable_original = is_prev_missing
        df_hujan['is_change_unreliable'] = is_untestable_original.shift(1).fillna(False)

      # --- Hitung INTERVAL_COLUMN FINAL ---
    conditions = [
        is_reset_detected,             
        is_prev_missing,                
    ]
    choices = [
        df_hujan[CUMULATIVE_COLUMN],    
        np.nan,                         
    ]
    df_hujan[INTERVAL_COLUMN] = np.select(conditions, choices, default=df_hujan['raw_diff_internal'])

    print("✅ (Hujan) Perhitungan interval & helper selesai.")
    # =================================================================

    # --- Siapkan kolom flag ---
    if FLAG_COLUMN in df_hujan.columns:
        print(f"⚠️ Kolom '{FLAG_COLUMN}' sudah ada, akan diinisialisasi ulang.")
    df_hujan[FLAG_COLUMN] = np.nan
    print("\n🔬 (Hujan) Menjalankan Quality Control...")

    # --- Jalankan QC (prioritas flagging) ---
    df_hujan = handle_original_missing_data(df_hujan, ORIGINAL_CUMULATIVE_COLUMN, FLAG_COLUMN) # Flag 9
    df_hujan = range_check(df_hujan, INTERVAL_COLUMN, FLAG_COLUMN, RR_MIN_RANGE, RR_MAX_RANGE) # Flag 1
    df_hujan = unexpected_drop_test(df_hujan, 'raw_diff_internal', FLAG_COLUMN, UNEXPECTED_DROP_THRESHOLD) # Flag 5
    df_hujan = fixed_rapid_change_test(df_hujan, INTERVAL_COLUMN, FLAG_COLUMN, RAPID_CHANGE_THRESHOLD) # Flag 3
    df_hujan = fixed_spike_test(df_hujan, INTERVAL_COLUMN, FLAG_COLUMN, RAPID_CHANGE_THRESHOLD) # Flag 4
    df_hujan = flat_line_test(df_hujan, INTERVAL_COLUMN, FLAG_COLUMN, FLAT_LINE_WINDOW, FLAT_LINE_MIN_VALUE) # Flag 2

    # --- Hapus kolom helper ---
    kolom_dihapus = [
        INTERVAL_COLUMN, ORIGINAL_CUMULATIVE_COLUMN, 'raw_diff_internal',
        'is_reset_event_internal', 'is_hardcoded_reset_time_internal', 'is_change_unreliable'
    ]
    df_hujan.drop(columns=kolom_dihapus, inplace=True, errors='ignore')
    print(f"\nKolom internal sementara telah dihapus.")

    # --- Ringkasan hasil ---
    summary_qc(df_hujan, FLAG_COLUMN)

    # Gabungkan kolom flag kembali ke DataFrame asli (df)
    df[FLAG_COLUMN] = df_hujan[FLAG_COLUMN]

    return df 