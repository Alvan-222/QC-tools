QC-tools
Skrip Python untuk melakukan Quality Control (QC) pada data observasi iklim yang berasal dari peralatan otomatis. Paket kode ini dirancang untuk memeriksa tiga parameter utama:
 - Curah Hujan (Rainfall) – pemeriksaan akumulasi harian untuk mendeteksi nilai tidak logis, missing, atau lonjakan yang tidak wajar.
- Tekanan Udara (Air Pressure) – pemeriksaan data 10-menitan untuk mendeteksi nilai ekstrim, data stagnan (flatline), serta perubahan mendadak.
- Radiasi Matahari Rata-Rata (Solar Radiation) – pemeriksaan data 10-menitan untuk mengidentifikasi pola tidak wajar, nilai yang tidak mungkin secara fisik, dan gangguan sensor.

Skrip QC ini mencakup beberapa fungsi utama:
- Validasi format dan kelengkapan data
- Pemeriksaan batas wajar (range check)
- Deteksi data stagnan (flatline check)
- Deteksi perubahan cepat atau spike (rapid/spike check)
- Pemberian flag otomatis untuk anomali
- Output data hasil QC dengan penandaan (flag) untuk tiap parameter

Repository ini dikembangkan untuk membantu proses QC data observasi iklim agar lebih akurat, konsisten, dan sesuai standar WMO, serta mempermudah analisis lanjutan di lingkungan BMKG.
