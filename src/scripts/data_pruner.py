#!/usr/bin/env python3
# data_pruner.py

import pandas as pd
import os

print("=== INICIANDO DATA PRUNER ===")

# 1. Carpeta de GTFS descomprimido
GTFS_DIR = "src/data/gtfs_be"
OUT_DIR  = "src/data/gtfs_pruned"
print(f"Directorio de entrada: {GTFS_DIR}")
print(f"Directorio de salida: {OUT_DIR}")
os.makedirs(OUT_DIR, exist_ok=True)
print(f"Directorio {OUT_DIR} creado/verificado")

# 2. Definir, para cada archivo, qué columnas mantener
KEEP = {
    "agency.txt":      ["agency_id","agency_name","agency_url","agency_timezone"],
    "stops.txt":       ["stop_id","stop_name","stop_lat","stop_lon"],
    "routes.txt":      ["route_id","route_short_name","route_long_name","route_type"],
    "trips.txt":       ["route_id","service_id","trip_id","direction_id","shape_id"],
    "stop_times.txt":  ["trip_id","arrival_time","departure_time","stop_id","stop_sequence"],
    "calendar.txt":    ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"],
    "calendar_dates.txt": ["service_id","date","exception_type"],
    "shapes.txt":      ["shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled"],
}

# 3. Procesar cada archivo
print(f"\n=== PROCESANDO {len(KEEP)} ARCHIVOS ===")
for fname, cols in KEEP.items():
    print(f"\nProcesando {fname}...")
    path_in  = os.path.join(GTFS_DIR, fname)
    path_out = os.path.join(OUT_DIR, fname)
    
    if not os.path.exists(path_in):
        print(f"  ⚠️  {path_in} no existe, saltando...")
        continue
    
    print(f"  📂 Leyendo {path_in}...")
    df = pd.read_csv(path_in, dtype=str)
    print(f"  📊 Archivo original: {len(df.columns)} columnas, {len(df)} filas")
    
    # Filtrar columnas existentes
    to_keep = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]
    
    if missing:
        print(f"  ⚠️  Columnas faltantes: {missing}")
    
    pruned  = df[to_keep]
    pruned.to_csv(path_out, index=False, encoding="utf-8")
    print(f"  ✅ Guardado {path_out} con {len(pruned.columns)} columnas y {len(pruned)} filas")

print(f"\n=== DATA PRUNER COMPLETADO ===")
print(f"Archivos procesados guardados en {OUT_DIR}/")

