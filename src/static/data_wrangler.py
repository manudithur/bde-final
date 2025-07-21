#!/usr/bin/env python3
# data_wrangler.py

import pandas as pd
from datetime import timedelta

print("=== INICIANDO DATA WRANGLER ===")

# Carga y descartado de filas incompletas
print("📂 Cargando gtfs_pruned/stop_times.txt...")
df = pd.read_csv("src/data/gtfs_pruned/stop_times.txt", dtype=str)
print(f"📊 Archivo original: {len(df)} filas")

print("🧹 Descartando filas con tiempos incompletos...")
original_len = len(df)
df = df.dropna(subset=["arrival_time", "departure_time"])
print(f"📊 Después de limpieza: {len(df)} filas ({original_len - len(df)} filas descartadas)")

def to_seconds(t):
    h, m, s = str(t).split(":")
    return int(h)*3600 + int(m)*60 + int(s)

def to_hhmmss(sec):
    hours = sec // 3600
    minutes = (sec % 3600) // 60
    seconds = sec % 60
    if hours >= 24:
        print(f"⚠️ Tiempo de día siguiente detectado: {hours:02d}:{minutes:02d}:{seconds:02d}")
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

print("🔄 Convirtiendo tiempos a segundos...")
df["arrival_s"]   = df["arrival_time"].apply(to_seconds)
df["departure_s"] = df["departure_time"].apply(to_seconds)
print("✅ Conversión completada")

print("🔧 Ajustando anomalías y validando tiempos...")
out = []
duplicates_fixed = 0
sequence_fixes = 0
departure_fixes = 0
discarded_trips = 0
total_trips = df['trip_id'].nunique()
processed_trips = 0

for trip_id, g in df.groupby("trip_id", sort=False):
    processed_trips += 1
    if processed_trips % 1000 == 0:
        print(f"  📍 Procesando trip {processed_trips}/{total_trips}...")
    
    g = g.sort_values("stop_sequence").copy()
    trip_fixes = 0
    
    # Check for timestamp ordering violations in stop_sequence
    has_timestamp_violation = False
    for i in range(len(g)-1):
        if g.iloc[i+1]["arrival_s"] < g.iloc[i]["arrival_s"]:
            has_timestamp_violation = True
            break
    
    if has_timestamp_violation:     
        discarded_trips += 1
        continue
    
    # Fix departure_time < arrival_time within each stop
    for i in range(len(g)):
        if g.iloc[i]["departure_s"] < g.iloc[i]["arrival_s"]:
            idx = g.index[i]
            g.at[idx, "departure_s"] = g.at[idx, "arrival_s"]
            departure_fixes += 1
    
    # Fix duplicate timestamps
    for i in range(len(g)-1):
        if g.iloc[i]["arrival_s"] == g.iloc[i+1]["arrival_s"]:
            idx = g.index[i+1]
            g.at[idx, "arrival_s"]   += 1
            g.at[idx, "departure_s"] += 1
            trip_fixes += 1
    
    # Ensure monotonic increasing sequence within trip
    for i in range(len(g)-1):
        curr_departure = g.iloc[i]["departure_s"]
        next_arrival = g.iloc[i+1]["arrival_s"]
        
        if next_arrival <= curr_departure:
            idx = g.index[i+1]
            g.at[idx, "arrival_s"] = curr_departure + 60  # Add 1 minute
            g.at[idx, "departure_s"] = max(g.at[idx, "departure_s"], g.at[idx, "arrival_s"])
            sequence_fixes += 1
    
    if trip_fixes > 0:
        duplicates_fixed += trip_fixes
    
    out.append(g)

fixed = pd.concat(out, ignore_index=True)
print(f"✅ Anomalías corregidas:")
print(f"  - {duplicates_fixed} timestamps duplicados ajustados")
print(f"  - {departure_fixes} tiempos de salida corregidos (salida < llegada)")
print(f"  - {sequence_fixes} secuencias de tiempo corregidas")
print(f"  - {discarded_trips} trips descartados por violación de orden temporal")
print(f"📊 Trips procesados: {total_trips - discarded_trips}/{total_trips}")

# Reconstruye strings y guarda
print("🔄 Reconstruyendo strings de tiempo...")
fixed["arrival_time"]   = fixed["arrival_s"].apply(to_hhmmss)
fixed["departure_time"] = fixed["departure_s"].apply(to_hhmmss)

print("💾 Guardando archivo corregido...")
output_df = fixed[["trip_id","arrival_time","departure_time","stop_id","stop_sequence"]]
output_df.to_csv("src/data/gtfs_pruned/stop_times.txt", index=False, encoding="utf-8")

print(f"✅ Archivo guardado: src/data/gtfs_pruned/stop_times.txt")
print(f"📊 Filas finales: {len(output_df)}")
print("\n=== DATA WRANGLER COMPLETADO ===")
print(f"Resumen de correcciones en {total_trips} trips:")
print(f"  - {duplicates_fixed} timestamps duplicados")
print(f"  - {departure_fixes} tiempos de salida inválidos")
print(f"  - {sequence_fixes} secuencias temporales incorrectas")
print(f"  - {discarded_trips} trips descartados por violación de orden temporal")
print(f"📊 Trips válidos restantes: {total_trips - discarded_trips}/{total_trips}")

