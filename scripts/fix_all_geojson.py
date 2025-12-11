"""
Script para corrigir problemas comuns em arquivos GeoJSON para compatibilidade com ArcGIS Pro.

Problemas corrigidos:
1. Coordenadas vazias ["", ""] -> geometria null
2. Campos numéricos (latitude/longitude) como strings vazias "" -> null
3. Verifica fechamento correto do arquivo
"""

import json
import os
from pathlib import Path

def detectar_encoding(caminho_arquivo):
    """Tenta detectar o encoding do arquivo."""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    for encoding in encodings:
        try:
            with open(caminho_arquivo, 'r', encoding=encoding) as f:
                json.load(f)  # Tenta fazer parse também
            return encoding
        except (UnicodeDecodeError, UnicodeError, json.JSONDecodeError):
            continue
    return 'utf-8'  # fallback

def corrigir_geojson(caminho_arquivo, caminho_destino):
    """
    Corrige problemas em um arquivo GeoJSON e salva na pasta de destino.
    
    Args:
        caminho_arquivo: Caminho para o arquivo GeoJSON a ser corrigido
        caminho_destino: Caminho onde salvar o arquivo corrigido
    
    Returns:
        dict: Estatísticas das correções realizadas
    """
    print(f"\n{'='*60}")
    print(f"Processando: {caminho_arquivo}")
    print(f"Destino: {caminho_destino}")
    print(f"{'='*60}")
    
    # Detectar encoding
    encoding = detectar_encoding(caminho_arquivo)
    print(f"Encoding detectado: {encoding}")
    
    # Ler o arquivo
    try:
        with open(caminho_arquivo, 'r', encoding=encoding) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERRO: Arquivo JSON invalido - {e}")
        return None
    except Exception as e:
        print(f"ERRO ao ler arquivo: {e}")
        return None
    
    # Estatísticas
    stats = {
        'coordenadas_vazias': 0,
        'latitude_vazia': 0,
        'longitude_vazia': 0,
        'total_features': len(data.get('features', []))
    }
    
    # Processar features
    features_corrigidas = 0
    for feature in data.get('features', []):
        corrigido = False
        
        # Verificar e corrigir geometria com coordenadas vazias
        if (feature.get('geometry') and 
            isinstance(feature['geometry'], dict) and
            feature['geometry'].get('coordinates') == ["", ""]):
            feature['geometry'] = None
            stats['coordenadas_vazias'] += 1
            corrigido = True
        
        # Verificar e corrigir campos latitude e longitude vazios
        if feature.get('properties'):
            props = feature['properties']
            if props.get('latitude') == "":
                props['latitude'] = None
                stats['latitude_vazia'] += 1
                corrigido = True
            if props.get('longitude') == "":
                props['longitude'] = None
                stats['longitude_vazia'] += 1
                corrigido = True
        
        if corrigido:
            features_corrigidas += 1
    
    # Exibir estatísticas
    print(f"Total de features: {stats['total_features']}")
    print(f"Features corrigidas: {features_corrigidas}")
    print(f"  - Coordenadas vazias: {stats['coordenadas_vazias']}")
    print(f"  - Latitude vazia: {stats['latitude_vazia']}")
    print(f"  - Longitude vazia: {stats['longitude_vazia']}")
    
    # Salvar arquivo corrigido na pasta de destino
    # Criar diretório de destino se não existir
    caminho_destino.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Salvando arquivo corrigido em: {caminho_destino}")
    with open(caminho_destino, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    if features_corrigidas > 0:
        print(f"[OK] Arquivo corrigido e salvo com sucesso!")
    else:
        print(f"[OK] Arquivo salvo (nenhuma correcao necessaria).")
    
    return stats


def main():
    """Processa todos os arquivos GeoJSON na pasta dados/brutos/ e salva em dados/clean/"""
    
    # Diretório base do projeto
    base_dir = Path(__file__).parent.parent
    brutos_dir = base_dir / 'dados' / 'brutos'
    clean_dir = base_dir / 'dados' / 'clean'
    
    if not brutos_dir.exists():
        print(f"ERRO: Diretorio nao encontrado: {brutos_dir}")
        return
    
    # Criar pasta de destino se não existir
    clean_dir.mkdir(parents=True, exist_ok=True)
    print(f"Pasta de destino: {clean_dir}")
    
    # Encontrar todos os arquivos GeoJSON
    arquivos_geojson = list(brutos_dir.glob('*.json'))
    
    if not arquivos_geojson:
        print(f"Nenhum arquivo GeoJSON encontrado em {brutos_dir}")
        return
    
    print(f"\n{'#'*60}")
    print(f"Correcao de Arquivos GeoJSON")
    print(f"{'#'*60}")
    print(f"Origem: {brutos_dir}")
    print(f"Destino: {clean_dir}")
    print(f"Arquivos encontrados: {len(arquivos_geojson)}")
    
    # Processar cada arquivo
    total_stats = {
        'arquivos_processados': 0,
        'arquivos_corrigidos': 0,
        'total_coordenadas_vazias': 0,
        'total_latitude_vazia': 0,
        'total_longitude_vazia': 0
    }
    
    for arquivo in arquivos_geojson:
        # Criar caminho de destino mantendo o mesmo nome do arquivo
        arquivo_destino = clean_dir / arquivo.name
        stats = corrigir_geojson(arquivo, arquivo_destino)
        if stats:
            total_stats['arquivos_processados'] += 1
            if (stats['coordenadas_vazias'] > 0 or 
                stats['latitude_vazia'] > 0 or 
                stats['longitude_vazia'] > 0):
                total_stats['arquivos_corrigidos'] += 1
            total_stats['total_coordenadas_vazias'] += stats['coordenadas_vazias']
            total_stats['total_latitude_vazia'] += stats['latitude_vazia']
            total_stats['total_longitude_vazia'] += stats['longitude_vazia']
    
    # Resumo final
    print(f"\n{'#'*60}")
    print(f"RESUMO FINAL")
    print(f"{'#'*60}")
    print(f"Arquivos processados: {total_stats['arquivos_processados']}")
    print(f"Arquivos corrigidos: {total_stats['arquivos_corrigidos']}")
    print(f"Total de correcoes:")
    print(f"  - Coordenadas vazias: {total_stats['total_coordenadas_vazias']}")
    print(f"  - Latitude vazia: {total_stats['total_latitude_vazia']}")
    print(f"  - Longitude vazia: {total_stats['total_longitude_vazia']}")
    print(f"\nArquivos salvos em: {clean_dir}")
    print(f"[OK] Processamento concluido!")


if __name__ == '__main__':
    main()

