"""
Script para contar ocorrências por bairro.

Este script:
1. Lê a geometria dos bairros de um shapefile
2. Lê as ocorrências de um arquivo GeoJSON
3. Conta as ocorrências por bairro (total e por tipo)
4. Cria um novo shapefile com as contagens
5. Salva o resultado em dados/processados/
"""

import json
import geopandas as gpd
import pandas as pd
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def encontrar_shapefile_bairros(base_dir):
    """
    Tenta encontrar o shapefile de bairros em diferentes locais.
    
    Returns:
        Path: Caminho para o shapefile (.shp) ou None se não encontrado
    """
    possiveis_caminhos = [
        # Dentro de um diretório
        base_dir / 'dados' / 'brutos' / 'camadas' / 'Limite_de_Bairros' / 'Limite_de_Bairros.shp',
        base_dir / 'dados' / 'camadas' / 'Limite_de_Bairros' / 'Limite_de_Bairros.shp',
        # Arquivos soltos
        base_dir / 'dados' / 'camadas' / 'Limite_de_Bairros.shp',
        base_dir / 'dados' / 'brutos' / 'camadas' / 'Limite_de_Bairros.shp',
    ]
    
    for caminho in possiveis_caminhos:
        if caminho.exists():
            # Verificar se os arquivos complementares existem
            shx = caminho.with_suffix('.shx')
            dbf = caminho.with_suffix('.dbf')
            if shx.exists() and dbf.exists():
                return caminho
            else:
                print(f"AVISO: Shapefile encontrado mas faltam componentes:")
                print(f"  .shp: {caminho.exists()}")
                print(f"  .shx: {shx.exists()}")
                print(f"  .dbf: {dbf.exists()}")
                # Tenta mesmo assim
                return caminho
    
    # Se não encontrou .shp, tenta usar .shx como referência
    possiveis_shx = [
        base_dir / 'dados' / 'brutos' / 'camadas' / 'Limite_de_Bairros' / 'Limite_de_Bairros.shx',
        base_dir / 'dados' / 'camadas' / 'Limite_de_Bairros' / 'Limite_de_Bairros.shx',
        base_dir / 'dados' / 'camadas' / 'Limite_de_Bairros.shx',
        base_dir / 'dados' / 'brutos' / 'camadas' / 'Limite_de_Bairros.shx',
    ]
    
    for caminho_shx in possiveis_shx:
        if caminho_shx.exists():
            # Tenta encontrar o .shp correspondente
            caminho_shp = caminho_shx.with_suffix('.shp')
            if caminho_shp.exists():
                return caminho_shp
            else:
                print(f"AVISO: Apenas arquivo .shx encontrado: {caminho_shx}")
                print("Um shapefile completo precisa de .shp, .shx e .dbf")
                # Tenta ler mesmo assim (pode não funcionar)
                return caminho_shx
    
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
        gpd.GeoDataFrame: GeoDataFrame com as ocorrências
    """
    print(f"Lendo ocorrências de: {caminho_geojson}")
    
    # Detectar encoding
    encoding = detectar_encoding(caminho_geojson)
    print(f"Encoding detectado: {encoding}")
    
    try:
        # Ler como JSON primeiro para garantir encoding correto
        with open(caminho_geojson, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # Filtrar features com geometrias inválidas
        features_validas = []
        features_invalidas = 0
        
        for feature in data.get('features', []):
            geometry = feature.get('geometry')
            
            # Verificar se a geometria é válida
            if geometry is None:
                features_invalidas += 1
                continue
            
            # Verificar coordenadas vazias ou inválidas
            if isinstance(geometry, dict) and 'coordinates' in geometry:
                coords = geometry['coordinates']
                # Verificar se são coordenadas vazias ["", ""] ou strings vazias
                if (isinstance(coords, list) and len(coords) >= 2 and 
                    (coords == ["", ""] or 
                     any(isinstance(c, str) and c == "" for c in coords[:2]))):
                    features_invalidas += 1
                    continue
                
                # Verificar se as coordenadas podem ser convertidas para float
                try:
                    if len(coords) >= 2:
                        float(coords[0])
                        float(coords[1])
                except (ValueError, TypeError):
                    features_invalidas += 1
                    continue
            
            features_validas.append(feature)
        
        if features_invalidas > 0:
            print(f"Aviso: {features_invalidas} features com geometrias inválidas foram ignoradas")
        
        # Converter para GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(features_validas)
        print(f"Total de ocorrências carregadas: {len(gdf)}")
        return gdf
    except Exception as e:
        print(f"Erro ao ler GeoJSON: {e}")
        raise


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


def contar_ocorrencias_por_bairro(gdf_ocorrencias):
    """
    Conta ocorrências por bairro (total, por tipo e por estação).
    
    Args:
        gdf_ocorrencias: GeoDataFrame com as ocorrências
        
    Returns:
        dict: Dicionário com contagens por bairro
        dict: Dicionário com contagens por tipo por bairro
        dict: Dicionário com contagens por estação por bairro
        dict: Dicionário com contagens por tipo e estação por bairro
    """
    print("\nContando ocorrências por bairro...")
    
    # Verificar se as colunas necessárias existem
    if 'bairro' not in gdf_ocorrencias.columns:
        raise ValueError("Coluna 'bairro' não encontrada nas ocorrências")
    
    if 'tipo' not in gdf_ocorrencias.columns:
        print("Aviso: Coluna 'tipo' não encontrada. Contando apenas total por bairro.")
        tipo_disponivel = False
    else:
        tipo_disponivel = True
    
    # Verificar se há coluna de data
    coluna_data = None
    for col in ['data_inicio', 'data_fim', 'data_particao']:
        if col in gdf_ocorrencias.columns:
            coluna_data = col
            break
    
    if coluna_data is None:
        print("Aviso: Coluna de data não encontrada. Não será possível calcular contagens por estação.")
        estacao_disponivel = False
    else:
        estacao_disponivel = True
        print(f"Usando coluna '{coluna_data}' para determinar estação do ano")
    
    # Contagem total por bairro
    contagem_total = gdf_ocorrencias.groupby('bairro').size().to_dict()
    
    # Contagem por tipo por bairro
    contagem_por_tipo = defaultdict(lambda: defaultdict(int))
    
    # Contagem por estação por bairro
    contagem_por_estacao = defaultdict(lambda: defaultdict(int))
    
    # Contagem por tipo e estação por bairro
    contagem_por_tipo_estacao = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    if tipo_disponivel or estacao_disponivel:
        for _, row in gdf_ocorrencias.iterrows():
            bairro = row['bairro']
            if pd.isna(bairro):
                continue
            
            # Determinar estação
            estacao = None
            if estacao_disponivel:
                data_valor = row[coluna_data]
                estacao = determinar_estacao_hemisferio_sul(data_valor)
            
            # Contagem por tipo
            if tipo_disponivel:
                tipo = row['tipo']
                if pd.notna(tipo):
                    contagem_por_tipo[bairro][tipo] += 1
                    
                    # Contagem por tipo e estação
                    if estacao:
                        contagem_por_tipo_estacao[bairro][tipo][estacao] += 1
            
            # Contagem por estação
            if estacao:
                contagem_por_estacao[bairro][estacao] += 1
    
    print(f"Bairros únicos encontrados: {len(contagem_total)}")
    
    return (contagem_total, 
            dict(contagem_por_tipo),
            dict(contagem_por_estacao),
            dict(contagem_por_tipo_estacao))


def adicionar_contagens_ao_shapefile(gdf_bairros, contagem_total, contagem_por_tipo, 
                                     contagem_por_estacao=None, contagem_por_tipo_estacao=None):
    """
    Adiciona as contagens ao GeoDataFrame dos bairros.
    
    Args:
        gdf_bairros: GeoDataFrame com os bairros
        contagem_total: Dicionário com contagem total por bairro
        contagem_por_tipo: Dicionário com contagem por tipo por bairro
        
    Returns:
        gpd.GeoDataFrame: GeoDataFrame atualizado com as contagens
    """
    print("\nAdicionando contagens ao shapefile de bairros...")
    
    # Identificar a coluna que contém o nome do bairro
    # Tentar diferentes nomes possíveis
    coluna_bairro = None
    possiveis_nomes = ['bairro', 'BAIRRO', 'Bairro', 'NOME', 'nome', 'NOME_BAIRRO', 'nome_bairro']
    
    for nome in possiveis_nomes:
        if nome in gdf_bairros.columns:
            coluna_bairro = nome
            break
    
    if coluna_bairro is None:
        print("Colunas disponíveis no shapefile:", list(gdf_bairros.columns))
        # Se não encontrar, usar a primeira coluna de texto
        for col in gdf_bairros.columns:
            if gdf_bairros[col].dtype == 'object':
                coluna_bairro = col
                print(f"Usando coluna '{coluna_bairro}' como identificador de bairro")
                break
    
    if coluna_bairro is None:
        raise ValueError("Não foi possível identificar a coluna de bairro no shapefile")
    
    print(f"Usando coluna '{coluna_bairro}' para identificar bairros")
    
    # Normalizar nomes dos bairros (remover espaços extras, converter para string)
    gdf_bairros['bairro_normalizado'] = gdf_bairros[coluna_bairro].astype(str).str.strip()
    contagem_total_normalizada = {str(k).strip(): v for k, v in contagem_total.items()}
    contagem_por_tipo_normalizada = {
        str(bairro).strip(): tipos 
        for bairro, tipos in contagem_por_tipo.items()
    }
    
    # Normalizar contagens por estação
    contagem_por_estacao_normalizada = {}
    if contagem_por_estacao:
        contagem_por_estacao_normalizada = {
            str(bairro).strip(): {str(est).strip(): v for est, v in estacoes.items()}
            for bairro, estacoes in contagem_por_estacao.items()
        }
    
    # Normalizar contagens por tipo e estação
    contagem_por_tipo_estacao_normalizada = {}
    if contagem_por_tipo_estacao:
        contagem_por_tipo_estacao_normalizada = {
            str(bairro).strip(): {
                str(tipo).strip(): {str(est).strip(): v for est, v in estacoes.items()}
                for tipo, estacoes in tipos_dict.items()
            }
            for bairro, tipos_dict in contagem_por_tipo_estacao.items()
        }
    
    # Adicionar contagem total
    gdf_bairros['contagem_total'] = gdf_bairros['bairro_normalizado'].map(
        contagem_total_normalizada
    ).fillna(0).astype(int)
    
    # Obter todos os tipos únicos
    todos_tipos = set()
    for tipos_dict in contagem_por_tipo_normalizada.values():
        todos_tipos.update(tipos_dict.keys())
    
    todos_tipos = sorted(list(todos_tipos))
    print(f"Tipos únicos encontrados: {todos_tipos}")
    
    # Mapear tipos para os nomes de propriedades solicitados
    mapeamento_tipos = {}
    for tipo in todos_tipos:
        if 'Alagamento' in tipo:
            nome_coluna = 'contagem_alagamento'
        elif "Bols" in tipo or "Bolsão" in tipo:
            nome_coluna = 'contagem_bolsao'
        elif "Lâmina" in tipo or "Lamina" in tipo:
            nome_coluna = 'contagem_lamina'
        else:
            # Para outros tipos, usar nome genérico
            tipo_limpo = tipo.replace(' ', '_').replace("'", '').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
            tipo_limpo = ''.join(c if c.isalnum() or c == '_' else '_' for c in tipo_limpo)
            nome_coluna = f'contagem_{tipo_limpo.lower()}'
        
        mapeamento_tipos[tipo] = nome_coluna
    
    # Adicionar contagem por tipo
    for tipo, nome_coluna in mapeamento_tipos.items():
        gdf_bairros[nome_coluna] = gdf_bairros['bairro_normalizado'].apply(
            lambda b: contagem_por_tipo_normalizada.get(b, {}).get(tipo, 0)
        )
    
    # Garantir que todas as colunas de contagem existam (mesmo que zero)
    if 'contagem_lamina' not in gdf_bairros.columns:
        gdf_bairros['contagem_lamina'] = 0
    if 'contagem_bolsao' not in gdf_bairros.columns:
        gdf_bairros['contagem_bolsao'] = 0
    if 'contagem_alagamento' not in gdf_bairros.columns:
        gdf_bairros['contagem_alagamento'] = 0
    
    # Calcular área em km²
    print("\nCalculando área dos bairros...")
    # Verificar se já existe uma coluna de área
    if 'st_areasha' in gdf_bairros.columns:
        # Assumir que está em m² (comum em shapefiles)
        # Converter para km² (dividir por 1.000.000)
        gdf_bairros['area_km2'] = gdf_bairros['st_areasha'] / 1_000_000
        print("Usando coluna 'st_areasha' (convertida de m² para km²)")
    else:
        # Calcular área a partir da geometria
        # Garantir que o CRS está definido (assumir WGS84 se não estiver)
        if gdf_bairros.crs is None:
            print("AVISO: CRS não definido. Assumindo WGS84 (EPSG:4326)")
            gdf_bairros.set_crs(epsg=4326, inplace=True)
        
        # Converter para um CRS projetado adequado para cálculo de área (ex: UTM)
        # Para o Rio de Janeiro, usar UTM Zone 23S (EPSG:31983)
        gdf_bairros_proj = gdf_bairros.to_crs(epsg=31983)
        # Calcular área em m² e converter para km²
        gdf_bairros['area_km2'] = gdf_bairros_proj.geometry.area / 1_000_000
        print("Área calculada a partir da geometria (convertida para km²)")
    
    # Calcular densidade (ocorrências por km²)
    print("Calculando densidade por km²...")
    # Evitar divisão por zero
    # Usar nome curto para caber no limite de 10 caracteres do shapefile
    gdf_bairros['dens_km2'] = gdf_bairros.apply(
        lambda row: row['contagem_total'] / row['area_km2'] if row['area_km2'] > 0 else 0,
        axis=1
    )
    
    # Calcular densidade por tipo
    print("Calculando densidade por tipo...")
    gdf_bairros['dens_lamina'] = gdf_bairros.apply(
        lambda row: row['contagem_lamina'] / row['area_km2'] if row['area_km2'] > 0 else 0,
        axis=1
    )
    gdf_bairros['dens_bolsao'] = gdf_bairros.apply(
        lambda row: row['contagem_bolsao'] / row['area_km2'] if row['area_km2'] > 0 else 0,
        axis=1
    )
    gdf_bairros['dens_alag'] = gdf_bairros.apply(
        lambda row: row['contagem_alagamento'] / row['area_km2'] if row['area_km2'] > 0 else 0,
        axis=1
    )
    
    # Adicionar contagens e densidades por estação
    if contagem_por_estacao_normalizada:
        print("\nAdicionando contagens por estação...")
        estacoes = ['verao', 'outono', 'inverno', 'primavera']
        
        for estacao in estacoes:
            # Contagem total por estação
            nome_col = f'cont_{estacao}'
            gdf_bairros[nome_col] = gdf_bairros['bairro_normalizado'].apply(
                lambda b: contagem_por_estacao_normalizada.get(b, {}).get(estacao, 0)
            )
            
            # Densidade total por estação
            nome_dens = f'dens_{estacao}'
            gdf_bairros[nome_dens] = gdf_bairros.apply(
                lambda row: row[nome_col] / row['area_km2'] if row['area_km2'] > 0 else 0,
                axis=1
            )
        
        # Adicionar contagens e densidades por tipo e estação
        if contagem_por_tipo_estacao_normalizada:
            print("Adicionando contagens por tipo e estação...")
            
            for tipo_nome, tipo_col in mapeamento_tipos.items():
                # Extrair nome curto do tipo
                if 'alagamento' in tipo_col:
                    tipo_curto = 'alag'
                elif 'bolsao' in tipo_col:
                    tipo_curto = 'bolsao'
                elif 'lamina' in tipo_col:
                    tipo_curto = 'lamina'
                else:
                    tipo_curto = tipo_col.replace('contagem_', '')[:6]
                
                for estacao in estacoes:
                    # Contagem por tipo e estação
                    nome_col = f'cont_{tipo_curto}_{estacao}'
                    gdf_bairros[nome_col] = gdf_bairros['bairro_normalizado'].apply(
                        lambda b: contagem_por_tipo_estacao_normalizada.get(b, {}).get(tipo_nome, {}).get(estacao, 0)
                    )
                    
                    # Densidade por tipo e estação
                    nome_dens = f'dens_{tipo_curto}_{estacao}'
                    gdf_bairros[nome_dens] = gdf_bairros.apply(
                        lambda row: row[nome_col] / row['area_km2'] if row['area_km2'] > 0 else 0,
                        axis=1
                    )
    
    # Remover coluna temporária
    gdf_bairros = gdf_bairros.drop(columns=['bairro_normalizado'])
    
    print(f"Contagens adicionadas. Total de bairros: {len(gdf_bairros)}")
    print(f"Bairros com ocorrências: {(gdf_bairros['contagem_total'] > 0).sum()}")
    densidades_validas = gdf_bairros[gdf_bairros['dens_km2'] > 0]['dens_km2']
    if len(densidades_validas) > 0:
        print(f"Densidade média: {densidades_validas.mean():.2f} ocorrências/km²")
    
    return gdf_bairros


def main():
    """Função principal."""
    # Diretório base do projeto
    base_dir = Path(__file__).parent.parent
    
    # Caminhos dos arquivos
    caminho_shapefile = encontrar_shapefile_bairros(base_dir)
    caminho_ocorrencias = base_dir / 'dados' / 'clean' / 'ocorrencias-geojson.json'
    caminho_saida = base_dir / 'dados' / 'processados' / 'Bairros_com_Contagem.shp'
    
    # Verificar se os arquivos existem
    if caminho_shapefile is None:
        print("ERRO: Shapefile de bairros não encontrado!")
        print("Procurando em:")
        print("  - dados/camadas/Limite_de_Bairros.shp")
        print("  - dados/brutos/camadas/Limite_de_Bairros.shp")
        return
    
    if not caminho_shapefile.exists():
        print(f"ERRO: Shapefile não encontrado: {caminho_shapefile}")
        return
    
    if not caminho_ocorrencias.exists():
        print(f"ERRO: Arquivo de ocorrências não encontrado: {caminho_ocorrencias}")
        return
    
    print(f"{'='*60}")
    print("Contagem de Ocorrências por Bairro")
    print(f"{'='*60}")
    print(f"Shapefile de bairros: {caminho_shapefile}")
    print(f"Arquivo de ocorrências: {caminho_ocorrencias}")
    print(f"Arquivo de saída: {caminho_saida}")
    print(f"{'='*60}\n")
    
    # Ler shapefile de bairros
    print("Lendo shapefile de bairros...")
    try:
        gdf_bairros = gpd.read_file(caminho_shapefile)
        print(f"Total de bairros carregados: {len(gdf_bairros)}")
        print(f"Colunas disponíveis: {list(gdf_bairros.columns)}")
    except Exception as e:
        print(f"ERRO ao ler shapefile: {e}")
        return
    
    # Ler ocorrências
    gdf_ocorrencias = ler_ocorrencias(caminho_ocorrencias)
    
    # Contar ocorrências por bairro
    contagem_total, contagem_por_tipo, contagem_por_estacao, contagem_por_tipo_estacao = contar_ocorrencias_por_bairro(gdf_ocorrencias)
    
    # Adicionar contagens ao shapefile
    gdf_resultado = adicionar_contagens_ao_shapefile(
        gdf_bairros, contagem_total, contagem_por_tipo, 
        contagem_por_estacao, contagem_por_tipo_estacao
    )
    
    # Criar diretório de saída se não existir
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    
    # Salvar shapefile
    print(f"\nSalvando shapefile em: {caminho_saida}")
    try:
        gdf_resultado.to_file(caminho_saida, encoding='utf-8')
        print(f"[OK] Shapefile salvo com sucesso!")
        print(f"\nResumo:")
        print(f"  - Total de bairros: {len(gdf_resultado)}")
        print(f"  - Bairros com ocorrências: {(gdf_resultado['contagem_total'] > 0).sum()}")
        print(f"  - Total de ocorrências: {gdf_resultado['contagem_total'].sum()}")
        print(f"  - Ocorrências de alagamento: {gdf_resultado['contagem_alagamento'].sum()}")
        print(f"  - Ocorrências de bolsão: {gdf_resultado['contagem_bolsao'].sum()}")
        print(f"  - Ocorrências de lâmina: {gdf_resultado['contagem_lamina'].sum()}")
        print(f"\nDensidade Total:")
        densidades_validas = gdf_resultado[gdf_resultado['dens_km2'] > 0]['dens_km2']
        if len(densidades_validas) > 0:
            print(f"  - Densidade média: {densidades_validas.mean():.2f} ocorrências/km²")
            print(f"  - Densidade máxima: {densidades_validas.max():.2f} ocorrências/km²")
            print(f"  - Densidade mínima: {densidades_validas.min():.2f} ocorrências/km²")
        
        print(f"\nDensidade por Tipo:")
        dens_lamina = gdf_resultado[gdf_resultado['dens_lamina'] > 0]['dens_lamina']
        if len(dens_lamina) > 0:
            print(f"  - Lâmina - média: {dens_lamina.mean():.2f}, máx: {dens_lamina.max():.2f}, mín: {dens_lamina.min():.2f} ocorrências/km²")
        
        dens_bolsao = gdf_resultado[gdf_resultado['dens_bolsao'] > 0]['dens_bolsao']
        if len(dens_bolsao) > 0:
            print(f"  - Bolsão - média: {dens_bolsao.mean():.2f}, máx: {dens_bolsao.max():.2f}, mín: {dens_bolsao.min():.2f} ocorrências/km²")
        
        dens_alag = gdf_resultado[gdf_resultado['dens_alag'] > 0]['dens_alag']
        if len(dens_alag) > 0:
            print(f"  - Alagamento - média: {dens_alag.mean():.2f}, máx: {dens_alag.max():.2f}, mín: {dens_alag.min():.2f} ocorrências/km²")
    except Exception as e:
        print(f"ERRO ao salvar shapefile: {e}")
        return
    
    print(f"\n{'='*60}")
    print("Processamento concluído!")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
