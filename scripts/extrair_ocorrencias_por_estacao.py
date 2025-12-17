"""
Script para extrair ocorrências por estação do ano.

Este script:
1. Lê as ocorrências de dados/clean/ocorrencias-geojson.json
2. Agrupa as ocorrências por estação do ano (hemisfério sul)
3. Salva 4 arquivos GeoJSON em dados/processados/ocorrencias/<estacao>.json
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict


def determinar_estacao_hemisferio_sul(data_valor):
    """
    Determina a estação do ano no hemisfério sul baseada na data.
    
    Estações no hemisfério sul (datas de início):
    - Outono: 20 de março
    - Inverno: 20 de junho
    - Primavera: 22 de setembro
    - Verão: 21 de dezembro
    
    Args:
        data_valor: String, Timestamp ou datetime com a data
        
    Returns:
        str: Nome da estação ('verao', 'outono', 'inverno', 'primavera') ou None
    """
    try:
        # Converter para datetime usando pandas (mais robusto)
        if pd.isna(data_valor):
            return None
        
        if isinstance(data_valor, pd.Timestamp):
            data = data_valor
        elif isinstance(data_valor, datetime):
            data = pd.Timestamp(data_valor)
        elif isinstance(data_valor, str):
            # Tentar parsear com pandas
            data = pd.to_datetime(data_valor, errors='coerce')
            if pd.isna(data):
                return None
        else:
            return None
        
        mes = data.month
        dia = data.day
        ano = data.year
        
        # Determinar estação baseado nas datas específicas
        # Verão: 21 dez a 19 mar
        if mes == 12 and dia >= 21:
            return 'verao'
        elif mes in [1, 2]:
            return 'verao'
        elif mes == 3 and dia < 20:
            return 'verao'
        
        # Outono: 20 mar a 19 jun
        elif mes == 3 and dia >= 20:
            return 'outono'
        elif mes in [4, 5]:
            return 'outono'
        elif mes == 6 and dia < 20:
            return 'outono'
        
        # Inverno: 20 jun a 21 set
        elif mes == 6 and dia >= 20:
            return 'inverno'
        elif mes in [7, 8]:
            return 'inverno'
        elif mes == 9 and dia < 22:
            return 'inverno'
        
        # Primavera: 22 set a 20 dez
        elif mes == 9 and dia >= 22:
            return 'primavera'
        elif mes in [10, 11]:
            return 'primavera'
        elif mes == 12 and dia < 21:
            return 'primavera'
        
        else:
            return None
    except Exception:
        return None


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


def ler_ocorrencias(caminho_geojson):
    """
    Lê as ocorrências do arquivo GeoJSON.
    
    Returns:
        dict: Dicionário com a estrutura GeoJSON
    """
    print(f"Lendo ocorrências de: {caminho_geojson}")
    
    # Detectar encoding
    encoding = detectar_encoding(caminho_geojson)
    print(f"Encoding detectado: {encoding}")
    
    try:
        with open(caminho_geojson, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        print(f"Total de features carregadas: {len(data.get('features', []))}")
        return data
    except Exception as e:
        print(f"Erro ao ler GeoJSON: {e}")
        raise


def extrair_ocorrencias_por_estacao(data_geojson):
    """
    Agrupa as ocorrências por estação do ano.
    
    Args:
        data_geojson: Dicionário com a estrutura GeoJSON
        
    Returns:
        dict: Dicionário com chaves sendo as estações e valores sendo listas de features
    """
    print("\nAgrupando ocorrências por estação do ano...")
    
    # Dicionário para armazenar features por estação
    ocorrencias_por_estacao = defaultdict(list)
    
    # Contadores
    ocorrencias_sem_data = 0
    ocorrencias_sem_estacao = 0
    
    # Verificar quais colunas de data estão disponíveis
    features = data_geojson.get('features', [])
    if not features:
        print("Aviso: Nenhuma feature encontrada no GeoJSON")
        return dict(ocorrencias_por_estacao)
    
    # Verificar primeira feature para identificar colunas de data
    primeira_feature = features[0]
    props = primeira_feature.get('properties', {})
    
    colunas_data = ['data_inicio', 'data_fim', 'data_particao']
    coluna_data_disponivel = None
    
    for col in colunas_data:
        if col in props:
            coluna_data_disponivel = col
            print(f"Usando coluna '{col}' para determinar estação do ano")
            break
    
    if coluna_data_disponivel is None:
        print("ERRO: Nenhuma coluna de data encontrada nas propriedades!")
        print(f"Colunas disponíveis: {list(props.keys())}")
        return dict(ocorrencias_por_estacao)
    
    # Processar cada feature
    for feature in features:
        props = feature.get('properties', {})
        data_valor = props.get(coluna_data_disponivel)
        
        if data_valor is None or (isinstance(data_valor, str) and data_valor.strip() == ''):
            ocorrencias_sem_data += 1
            continue
        
        estacao = determinar_estacao_hemisferio_sul(data_valor)
        
        if estacao is None:
            ocorrencias_sem_estacao += 1
            continue
        
        ocorrencias_por_estacao[estacao].append(feature)
    
    # Estatísticas
    print(f"\nEstatísticas:")
    print(f"  - Ocorrências sem data: {ocorrencias_sem_data}")
    print(f"  - Ocorrências sem estação determinada: {ocorrencias_sem_estacao}")
    print(f"\nOcorrências por estação:")
    for estacao in ['verao', 'outono', 'inverno', 'primavera']:
        count = len(ocorrencias_por_estacao.get(estacao, []))
        print(f"  - {estacao.capitalize()}: {count}")
    
    return dict(ocorrencias_por_estacao)


def salvar_geojson_por_estacao(ocorrencias_por_estacao, diretorio_saida):
    """
    Salva os GeoJSONs separados por estação.
    
    Args:
        ocorrencias_por_estacao: Dicionário com ocorrências por estação
        diretorio_saida: Path do diretório onde salvar os arquivos
    """
    print(f"\nSalvando arquivos GeoJSON em: {diretorio_saida}")
    
    # Criar diretório se não existir
    diretorio_saida.mkdir(parents=True, exist_ok=True)
    
    # Salvar cada estação
    for estacao, features in ocorrencias_por_estacao.items():
        caminho_arquivo = diretorio_saida / f"{estacao}.json"
        
        geojson_output = {
            "type": "FeatureCollection",
            "features": features
        }
        
        try:
            with open(caminho_arquivo, 'w', encoding='utf-8') as f:
                json.dump(geojson_output, f, ensure_ascii=False, indent=2)
            
            print(f"  [OK] {estacao.capitalize()}: {len(features)} ocorrências -> {caminho_arquivo}")
        except Exception as e:
            print(f"  [ERRO] Falha ao salvar {estacao}: {e}")


def main():
    """Função principal."""
    # Diretório base do projeto
    base_dir = Path(__file__).parent.parent
    
    # Caminhos dos arquivos
    caminho_ocorrencias = base_dir / 'dados' / 'clean' / 'ocorrencias-geojson.json'
    diretorio_saida = base_dir / 'dados' / 'processados' / 'ocorrencias'
    
    # Verificar se o arquivo existe
    if not caminho_ocorrencias.exists():
        print(f"ERRO: Arquivo de ocorrências não encontrado: {caminho_ocorrencias}")
        return
    
    print(f"{'='*60}")
    print("Extração de Ocorrências por Estação do Ano")
    print(f"{'='*60}")
    print(f"Arquivo de entrada: {caminho_ocorrencias}")
    print(f"Diretório de saída: {diretorio_saida}")
    print(f"{'='*60}\n")
    
    # Ler ocorrências
    data_geojson = ler_ocorrencias(caminho_ocorrencias)
    
    # Agrupar por estação
    ocorrencias_por_estacao = extrair_ocorrencias_por_estacao(data_geojson)
    
    # Salvar arquivos
    salvar_geojson_por_estacao(ocorrencias_por_estacao, diretorio_saida)
    
    print(f"\n{'='*60}")
    print("Processamento concluído!")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
