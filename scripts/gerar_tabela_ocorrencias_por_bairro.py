"""
Script to generate a table with occurrence type counts by neighborhood in Rio de Janeiro.

This script:
1. Reads occurrences from dados/clean/ocorrencias-geojson.json
2. Reads neighborhood boundaries from Limite_de_Bairros shapefile
3. Counts occurrences by type (Type I: Lâmina, Type II: Bolsão, Type III: Alagamento)
4. Generates a table in ABNT format (English)
5. Saves the table in dados/processados/ocorrencias_por_bairro/
"""

import json
import geopandas as gpd
import pandas as pd
from pathlib import Path
from collections import defaultdict


def encontrar_shapefile_bairros(base_dir):
    """
    Finds the neighborhood shapefile in different possible locations.
    
    Returns:
        Path: Path to the shapefile (.shp) or None if not found
    """
    possiveis_caminhos = [
        base_dir / 'dados' / 'brutos' / 'camadas' / 'Limite_de_Bairros' / 'Limite_de_Bairros.shp',
        base_dir / 'dados' / 'camadas' / 'Limite_de_Bairros' / 'Limite_de_Bairros.shp',
        base_dir / 'dados' / 'camadas' / 'Limite_de_Bairros.shp',
        base_dir / 'dados' / 'brutos' / 'camadas' / 'Limite_de_Bairros.shp',
    ]
    
    for caminho in possiveis_caminhos:
        if caminho.exists():
            shx = caminho.with_suffix('.shx')
            dbf = caminho.with_suffix('.dbf')
            if shx.exists() and dbf.exists():
                return caminho
            elif caminho.exists():
                return caminho
    
    return None


def detectar_encoding(caminho_arquivo):
    """Detects the encoding of a file."""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    for encoding in encodings:
        try:
            with open(caminho_arquivo, 'r', encoding=encoding) as f:
                json.load(f)
            return encoding
        except (UnicodeDecodeError, UnicodeError, json.JSONDecodeError):
            continue
    return 'utf-8'


def ler_ocorrencias(caminho_geojson):
    """
    Reads occurrences from GeoJSON file.
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with occurrences
    """
    print(f"Reading occurrences from: {caminho_geojson}")
    
    encoding = detectar_encoding(caminho_geojson)
    print(f"Detected encoding: {encoding}")
    
    try:
        with open(caminho_geojson, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # Filter features with invalid geometries
        features_validas = []
        features_invalidas = 0
        
        for feature in data.get('features', []):
            geometry = feature.get('geometry')
            
            if geometry is None:
                features_invalidas += 1
                continue
            
            if isinstance(geometry, dict) and 'coordinates' in geometry:
                coords = geometry['coordinates']
                if (isinstance(coords, list) and len(coords) >= 2 and 
                    (coords == ["", ""] or 
                     any(isinstance(c, str) and c == "" for c in coords[:2]))):
                    features_invalidas += 1
                    continue
                
                try:
                    if len(coords) >= 2:
                        float(coords[0])
                        float(coords[1])
                except (ValueError, TypeError):
                    features_invalidas += 1
                    continue
            
            features_validas.append(feature)
        
        if features_invalidas > 0:
            print(f"Warning: {features_invalidas} features with invalid geometries were ignored")
        
        gdf = gpd.GeoDataFrame.from_features(features_validas)
        print(f"Total occurrences loaded: {len(gdf)}")
        return gdf
    except Exception as e:
        print(f"Error reading GeoJSON: {e}")
        raise


def classificar_tipo_ocorrencia(tipo):
    """
    Classifies occurrence type according to the mapping:
    - Lâmina d'água -> Type I
    - Bolsão d'água em via -> Type II
    - Alagamento -> Type III
    
    Args:
        tipo: String with the occurrence type
        
    Returns:
        str: Type classification ('Type I', 'Type II', 'Type III', or 'Other')
    """
    if pd.isna(tipo):
        return 'Other'
    
    tipo_str = str(tipo).lower()
    
    if 'lâmina' in tipo_str or 'lamina' in tipo_str:
        return 'Type I'
    elif 'bols' in tipo_str or 'bolsão' in tipo_str:
        return 'Type II'
    elif 'alagamento' in tipo_str:
        return 'Type III'
    else:
        return 'Other'


def contar_ocorrencias_por_bairro(gdf_ocorrencias):
    """
    Counts occurrences by neighborhood and type.
    
    Args:
        gdf_ocorrencias: GeoDataFrame with occurrences
        
    Returns:
        dict: Dictionary with counts by neighborhood and type
    """
    print("\nCounting occurrences by neighborhood...")
    
    if 'bairro' not in gdf_ocorrencias.columns:
        raise ValueError("Column 'bairro' not found in occurrences")
    
    if 'tipo' not in gdf_ocorrencias.columns:
        print("Warning: Column 'tipo' not found. Counting only total by neighborhood.")
        tipo_disponivel = False
    else:
        tipo_disponivel = True
    
    # Count by neighborhood and type
    contagem_por_bairro = defaultdict(lambda: {
        'Type I': 0,
        'Type II': 0,
        'Type III': 0,
        'Other': 0,
        'Total': 0
    })
    
    for _, row in gdf_ocorrencias.iterrows():
        bairro = row['bairro']
        if pd.isna(bairro):
            continue
        
        bairro_str = str(bairro).strip()
        contagem_por_bairro[bairro_str]['Total'] += 1
        
        if tipo_disponivel:
            tipo = row['tipo']
            tipo_classificado = classificar_tipo_ocorrencia(tipo)
            contagem_por_bairro[bairro_str][tipo_classificado] += 1
    
    print(f"Unique neighborhoods found: {len(contagem_por_bairro)}")
    
    return dict(contagem_por_bairro)


def ler_bairros_shapefile(caminho_shapefile):
    """
    Reads neighborhood shapefile and returns a GeoDataFrame.
    
    Args:
        caminho_shapefile: Path to the shapefile
        
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with neighborhoods
    """
    print(f"Reading neighborhood shapefile: {caminho_shapefile}")
    
    try:
        gdf_bairros = gpd.read_file(caminho_shapefile)
        print(f"Total neighborhoods loaded: {len(gdf_bairros)}")
        print(f"Available columns: {list(gdf_bairros.columns)}")
        return gdf_bairros
    except Exception as e:
        print(f"Error reading shapefile: {e}")
        raise


def calcular_area_bairros(gdf_bairros, coluna_bairro):
    """
    Calculates area in km² for each neighborhood.
    
    Args:
        gdf_bairros: GeoDataFrame with neighborhoods
        coluna_bairro: Column name with neighborhood names
        
    Returns:
        dict: Dictionary mapping neighborhood name to area in km²
    """
    print("Calculating neighborhood areas...")
    
    # Check if area column exists
    if 'st_areasha' in gdf_bairros.columns:
        # Assume it's in m², convert to km²
        gdf_bairros['area_km2'] = gdf_bairros['st_areasha'] / 1_000_000
        print("Using 'st_areasha' column (converted from m² to km²)")
    else:
        # Calculate area from geometry
        if gdf_bairros.crs is None:
            print("WARNING: CRS not defined. Assuming WGS84 (EPSG:4326)")
            gdf_bairros.set_crs(epsg=4326, inplace=True)
        
        # Convert to projected CRS for area calculation (UTM Zone 23S for Rio)
        gdf_bairros_proj = gdf_bairros.to_crs(epsg=31983)
        # Calculate area in m² and convert to km²
        gdf_bairros['area_km2'] = gdf_bairros_proj.geometry.area / 1_000_000
        print("Area calculated from geometry (converted to km²)")
    
    # Create mapping from neighborhood name to area
    area_por_bairro = {}
    for _, row in gdf_bairros.iterrows():
        bairro = str(row[coluna_bairro]).strip()
        area = row['area_km2']
        if bairro and bairro.lower() != 'nan':
            area_por_bairro[bairro] = area
    
    return area_por_bairro


def criar_tabela_abnt(contagem_por_bairro, gdf_bairros=None):
    """
    Creates a table in ABNT format with occurrence counts by neighborhood.
    
    Args:
        contagem_por_bairro: Dictionary with counts by neighborhood
        gdf_bairros: Optional GeoDataFrame with neighborhood geometries
        
    Returns:
        pd.DataFrame: DataFrame with the table
    """
    print("\nCreating ABNT format table...")
    
    # Get list of all neighborhoods
    coluna_bairro = None
    area_por_bairro = {}
    
    if gdf_bairros is not None:
        # Try to find the neighborhood name column
        possiveis_nomes = ['nome', 'NOME', 'bairro', 'BAIRRO', 'NOME_BAIRRO', 'nome_bairro']
        
        for nome in possiveis_nomes:
            if nome in gdf_bairros.columns:
                coluna_bairro = nome
                break
        
        if coluna_bairro:
            # Calculate areas
            area_por_bairro = calcular_area_bairros(gdf_bairros, coluna_bairro)
            # Get all neighborhoods from shapefile and normalize
            bairros_shapefile = set()
            for bairro in gdf_bairros[coluna_bairro].astype(str).str.strip():
                if bairro and bairro.lower() != 'nan':
                    bairros_shapefile.add(bairro)
            
            # Create a mapping from normalized names to original occurrence names
            # This helps match neighborhoods even with slight name differences
            bairros_ocorrencias_normalizados = {}
            for bairro_oc in contagem_por_bairro.keys():
                bairro_normalizado = bairro_oc.lower().strip()
                bairros_ocorrencias_normalizados[bairro_normalizado] = bairro_oc
            
            # Match shapefile neighborhoods with occurrence neighborhoods
            bairros_finais = set()
            bairros_ocorrencias_usados = set()
            
            for bairro_shp in bairros_shapefile:
                bairro_shp_normalizado = bairro_shp.lower().strip()
                # Try to find matching occurrence neighborhood
                if bairro_shp_normalizado in bairros_ocorrencias_normalizados:
                    bairros_finais.add(bairros_ocorrencias_normalizados[bairro_shp_normalizado])
                    bairros_ocorrencias_usados.add(bairro_shp_normalizado)
                else:
                    # Use shapefile name if no match found
                    bairros_finais.add(bairro_shp)
            
            # Add any occurrence neighborhoods that weren't matched
            for bairro_oc in contagem_por_bairro.keys():
                if bairro_oc.lower().strip() not in bairros_ocorrencias_usados:
                    bairros_finais.add(bairro_oc)
            
            bairros = sorted(bairros_finais)
            print(f"Including all neighborhoods from shapefile: {len(bairros)} total")
        else:
            # Fallback: use only neighborhoods with occurrences
            bairros = sorted(contagem_por_bairro.keys())
            print(f"Could not find neighborhood column in shapefile. Using only neighborhoods with occurrences.")
    else:
        # Use only neighborhoods with occurrences
        bairros = sorted(contagem_por_bairro.keys())
    
    # Exclude "Barra da tijuca - teste"
    bairros = [b for b in bairros if b.lower() != "barra da tijuca - teste"]
    if "Barra da tijuca - teste" in contagem_por_bairro:
        print("Excluding 'Barra da tijuca - teste' from the table")
    
    # Create a normalized mapping for better matching
    contagem_normalizada = {}
    for bairro_oc, contagens in contagem_por_bairro.items():
        bairro_normalizado = bairro_oc.lower().strip()
        contagem_normalizada[bairro_normalizado] = (bairro_oc, contagens)
    
    # Prepare data for DataFrame
    dados = []
    for bairro in bairros:
        # Try to find counts for this neighborhood
        bairro_normalizado = bairro.lower().strip()
        if bairro_normalizado in contagem_normalizada:
            # Use the original occurrence name and its counts
            bairro_original, contagens = contagem_normalizada[bairro_normalizado]
            total = contagens['Total']
        else:
            # Neighborhood from shapefile with no occurrences
            bairro_original = bairro
            total = 0
            contagens = {
                'Type I': 0,
                'Type II': 0,
                'Type III': 0,
                'Total': 0
            }
        
        # Calculate density (occurrences per km²)
        # Try to find area for this neighborhood
        densidade = 0.0
        if area_por_bairro:
            # Try exact match first
            if bairro_original in area_por_bairro:
                area = area_por_bairro[bairro_original]
                if area > 0:
                    densidade = total / area
            else:
                # Try normalized match
                for bairro_shp, area in area_por_bairro.items():
                    if bairro_shp.lower().strip() == bairro_normalizado:
                        if area > 0:
                            densidade = total / area
                        break
        
        dados.append({
            'Neighborhood': bairro_original,
            'Type I (Lâmina)': contagens['Type I'],
            'Type II (Bolsão)': contagens['Type II'],
            'Type III (Alagamento)': contagens['Type III'],
            'Total': total,
            'Density (per km²)': round(densidade, 2) if densidade > 0 else 0.0
        })
    
    # Create DataFrame
    df = pd.DataFrame(dados)
    
    # Add totals row
    total_row = {
        'Neighborhood': 'TOTAL',
        'Type I (Lâmina)': df['Type I (Lâmina)'].sum(),
        'Type II (Bolsão)': df['Type II (Bolsão)'].sum(),
        'Type III (Alagamento)': df['Type III (Alagamento)'].sum(),
        'Total': df['Total'].sum(),
        'Density (per km²)': '-'  # Dash for totals row (density doesn't make sense for totals)
    }
    
    # Append totals row
    df_total = pd.DataFrame([total_row])
    df = pd.concat([df, df_total], ignore_index=True)
    
    # Convert density column to string to preserve '-' in totals row
    df['Density (per km²)'] = df['Density (per km²)'].astype(str)
    df.loc[df['Density (per km²)'] == '0.0', 'Density (per km²)'] = '0.00'
    df.loc[df['Density (per km²)'] == 'nan', 'Density (per km²)'] = '-'
    
    print(f"Table created with {len(df)} rows (including totals)")
    
    return df


def salvar_tabela(df, caminho_saida):
    """
    Saves the table in multiple formats (CSV and Excel).
    
    Args:
        df: DataFrame with the table
        caminho_saida: Path to save the table (without extension)
    """
    print(f"\nSaving table to: {caminho_saida}")
    
    # Save as CSV
    caminho_csv = caminho_saida.with_suffix('.csv')
    df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
    print(f"[OK] CSV saved: {caminho_csv}")
    
    # Save as Excel (if openpyxl is available)
    try:
        caminho_excel = caminho_saida.with_suffix('.xlsx')
        with pd.ExcelWriter(caminho_excel, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Occurrences by Neighborhood')
            # Format the Excel sheet
            worksheet = writer.sheets['Occurrences by Neighborhood']
            # Auto-adjust column widths
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                )
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)
        print(f"[OK] Excel saved: {caminho_excel}")
    except ImportError:
        print("[INFO] openpyxl not available. Excel file not created.")
        print("      Install with: pip install openpyxl")
    except Exception as e:
        print(f"[WARNING] Could not save Excel file: {e}")


def main():
    """Main function."""
    # Base directory
    base_dir = Path(__file__).parent.parent
    
    # File paths
    caminho_shapefile = encontrar_shapefile_bairros(base_dir)
    caminho_ocorrencias = base_dir / 'dados' / 'clean' / 'ocorrencias-geojson.json'
    caminho_saida_dir = base_dir / 'dados' / 'processados' / 'ocorrencias_por_bairro'
    caminho_saida = caminho_saida_dir / 'occurrence_counts_by_neighborhood'
    
    # Check if files exist
    if caminho_shapefile is None:
        print("WARNING: Neighborhood shapefile not found!")
        print("The script will continue using only occurrence data.")
        gdf_bairros = None
    elif not caminho_shapefile.exists():
        print(f"WARNING: Shapefile not found: {caminho_shapefile}")
        print("The script will continue using only occurrence data.")
        gdf_bairros = None
    else:
        gdf_bairros = ler_bairros_shapefile(caminho_shapefile)
    
    if not caminho_ocorrencias.exists():
        print(f"ERROR: Occurrence file not found: {caminho_ocorrencias}")
        return
    
    print(f"{'='*60}")
    print("Occurrence Counts by Neighborhood - Table Generator")
    print(f"{'='*60}")
    print(f"Occurrence file: {caminho_ocorrencias}")
    if gdf_bairros is not None:
        print(f"Neighborhood shapefile: {caminho_shapefile}")
    print(f"Output directory: {caminho_saida_dir}")
    print(f"{'='*60}\n")
    
    # Read occurrences
    gdf_ocorrencias = ler_ocorrencias(caminho_ocorrencias)
    
    # Count occurrences by neighborhood
    contagem_por_bairro = contar_ocorrencias_por_bairro(gdf_ocorrencias)
    
    # Create table
    df_tabela = criar_tabela_abnt(contagem_por_bairro, gdf_bairros)
    
    # Create output directory
    caminho_saida_dir.mkdir(parents=True, exist_ok=True)
    
    # Save table
    salvar_tabela(df_tabela, caminho_saida)
    
    # Print summary
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    print(f"Total neighborhoods: {len(df_tabela) - 1}")  # Exclude totals row
    print(f"Total occurrences: {df_tabela['Total'].iloc[-1]}")
    print(f"  - Type I (Lâmina): {df_tabela['Type I (Lâmina)'].iloc[-1]}")
    print(f"  - Type II (Bolsão): {df_tabela['Type II (Bolsão)'].iloc[-1]}")
    print(f"  - Type III (Alagamento): {df_tabela['Type III (Alagamento)'].iloc[-1]}")
    print(f"\nTable saved to: {caminho_saida_dir}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()

