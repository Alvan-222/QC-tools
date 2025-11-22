# =======================================================================
#
#   ▶️▶️▶️ KODE UTAMA (MAIN) ◀️◀️◀️
#
# =======================================================================

import pandas as pd
import sys 

# Impor fungsi spesifik dari setiap file modul
try:
    from qc_hujan import run_qc_hujan
    from qc_tekanan import run_qc_tekanan
    from qc_radiasi import run_qc_radiasi
except ImportError as e:
    print(f"❌ ERROR: Gagal mengimpor modul.")
    print("Pastikan file 'qc_hujan.py', 'qc_tekanan.py', dan 'qc_radiasi.py' berada di folder yang sama dengan 'main.py'.")
    print(f"Detail Error: {e}")
    sys.exit() 

# ==================================================
#   --- 📂 KONFIGURASI FILE I/O ---
# ==================================================

# File input (rr, pp_air, sr_avg)
INPUT_FILE = 'Data_AWS_Gabungan_QC_2.xlsx'

# File output 
OUTPUT_FILE = 'hasil_qc_data_lengkap(Tangsel).xlsx'

# ==================================================


def main():
    """
    Fungsi utama untuk menjalankan semua skrip QC secara berurutan
    pada satu file.
    """
    print("==================================================")
    print("🚀 MEMULAI PROSES QUALITY CONTROL (QC) DATA AWS 🚀")
    print("==================================================")
    
    # --- 1. Membaca File Input ---
    try:
        print(f"\n📥 Membaca file input tunggal: {INPUT_FILE}...")
        df = pd.read_excel(INPUT_FILE, engine="openpyxl")
        print(f"✅ Berhasil membaca {len(df)} baris data.")
    except FileNotFoundError:
        print(f"❌ ERROR: File input '{INPUT_FILE}' tidak ditemukan.")
        print("Pastikan nama file sudah benar dan file ada di folder yang sama.")
        return 
    except Exception as e:
        print(f"❌ ERROR saat membaca file Excel: {e}")
        return 
    
    # --- 2. Pembersihan & Persiapan Data ---
    print("\n🔄 Melakukan pembersihan dan persiapan data awal...")
    try:
        # Bersihkan nama kolom (menghapus spasi, dll.)
        df.columns = [c.strip().replace(' ', '_') for c in df.columns]
        
        # Konversi 'Tanggal' ke tipe datetime
        if 'Tanggal' not in df.columns:
            print("❌ ERROR: Kolom 'Tanggal' tidak ditemukan di file input.")
            return
            
        df['Tanggal'] = pd.to_datetime(df.get('Tanggal'), errors='coerce', utc=True)
        
        # Hapus baris dengan Tanggal yang tidak valid
        initial_rows = len(df)
        df.dropna(subset=['Tanggal'], inplace=True)
        if initial_rows > len(df):
            print(f"  - Membuang {initial_rows - len(df)} baris dengan tanggal tidak valid.")
            
        # Urutkan berdasarkan 'Tanggal'
        df = df.sort_values('Tanggal').reset_index(drop=True)
        print("✅ Data telah dibersihkan dan diurutkan berdasarkan 'Tanggal'.")
        
    except Exception as e:
        print(f"❌ ERROR saat persiapan data: {e}")
        return

    # --- 3. Menjalankan Modul QC secara Berurutan ---
    
    # --- QC Curah Hujan ---
    print("\n" + "=" * 50)
    print("🌧️ 1. Menjalankan QC Curah Hujan (rr)...")
    print("=" * 50)
    try:
        df = run_qc_hujan(df) 
        print("\n✅ QC Curah Hujan Selesai.")
    except Exception as e:
        print(f"❌ ERROR saat menjalankan QC Curah Hujan: {e}")
    
    # --- QC Tekanan Udara ---
    print("\n" + "=" * 50)
    print("🌬️ 2. Menjalankan QC Tekanan Udara (pp_air)...")
    print("=" * 50)
    try:
        df = run_qc_tekanan(df) 
        print("\n✅ QC Tekanan Udara Selesai.")
    except Exception as e:
        print(f"❌ ERROR saat menjalankan QC Tekanan Udara: {e}")
        
    # --- QC Radiasi Matahari ---
    print("\n" + "=" * 50)
    print("☀️ 3. Menjalankan QC Radiasi Matahari (sr_avg)...")
    print("=" * 50)
    try:
        df = run_qc_radiasi(df) 
        print("\n✅ QC Radiasi Matahari Selesai.")
    except Exception as e:
        print(f"❌ ERROR saat menjalankan QC Radiasi Matahari: {e}")
    
    # --- 4. Menyimpan File Output ---
    print("\n" + "=" * 50)
    print("💾 MENYIMPAN HASIL AKHIR")
    print("=" * 50)
    try:
        print(f"  - Menyiapkan kolom 'Tanggal' untuk Excel...")
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)
            
        print(f"  - Menyimpan DataFrame ke: {OUTPUT_FILE}...")
        df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
        
        print("\n🎉 SEMUA PROSES QC TELAH SELESAI DIJALANKAN.")
        print(f"File hasil disimpan di: {OUTPUT_FILE}")
        print("==================================================")
        
    except Exception as e:
        print(f"❌ ERROR saat menyimpan file output: {e}")
        print("Pastikan Anda memiliki izin tulis dan file tidak sedang dibuka.")


if __name__ == "__main__":
   
    main()

