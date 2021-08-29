import json
import pandas as pd

from Crud import *
from Connection import Connection


def bens(show_table=False, delete_if_exists=False):
    table_name = "bens"
    tab_cols = {
        "cols": {
            "NR_ORDEM_CANDIDATO": "integer",
            "SQ_CANDIDATO": "bigint",
            "CD_ELEICAO": "integer",
            "DT_ULTIMA_ATUALIZACAO": "date",
            "HH_ULTIMA_ATUALIZACAO": "time",
            "VR_BEM_CANDIDATO": "money",
            "DS_BEM_CANDIDATO": "varchar(512)",
            "CD_TIPO_BEM_CANDIDATO": "integer"
            
        },
        "PK": [
            "NR_ORDEM_CANDIDATO",
            "SQ_CANDIDATO",
            "CD_ELEICAO"
        ],
        "FK": {
            "SQ_CANDIDATO, CD_ELEICAO": (
                "candidatura", "SQ_CANDIDATO, CD_ELEICAO", "ON UPDATE CASCADE ON DELETE RESTRICT"
            ),
            "CD_TIPO_BEM_CANDIDATO": ("tipo_bem", "CD_TIPO_BEM_CANDIDATO", "ON UPDATE CASCADE ON DELETE RESTRICT")
        }
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            try:
                crud.drop_table()
                crud.create()
            except:
                print("Existe um Relacionamento impedindo a exclusão, NADA SERÁ FEITO")
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_bem[['NR_ORDEM_CANDIDATO', 'SQ_CANDIDATO', 'CD_ELEICAO', 'DT_ULTIMA_ATUALIZACAO', 'HH_ULTIMA_ATUALIZACAO',
                 'VR_BEM_CANDIDATO', 'DS_BEM_CANDIDATO', 'CD_TIPO_BEM_CANDIDATO']]

    tb['VR_BEM_CANDIDATO'] = tb['VR_BEM_CANDIDATO'].str.replace(",", ".")
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def candidato(show_table=False, delete_if_exists=False):
    table_name = "candidato"
    tab_cols = {
        "cols": {
            "SQ_CANDIDATO": "bigint",
            "NR_CANDIDATO": "integer",
            "NM_CANDIDATO": "varchar(128)",
            "NM_URNA_CANDIDATO": "varchar(128)",
            "NM_SOCIAL_CANDIDATO": "varchar(128)",
            "NR_CPF_CANDIDATO": "bigint",
            "NM_EMAIL": "varchar(128)",
            "DT_NASCIMENTO": "date",
            "SG_UF_NASCIMENTO": "varchar(2)",
            "CD_MUNICIPIO_NASCIMENTO": "integer",
            "NM_MUNICIPIO_NASCIMENTO": "varchar(128)",
            "NR_TITULO_ELEITORAL_CANDIDATO": "bigint",
            "CD_GENERO": "integer",
            "CD_GRAU_INSTRUCAO": "integer",
            "CD_ESTADO_CIVIL": "integer",
            "CD_COR_RACA": "integer",
            "CD_OCUPACAO": "integer",
            "CD_NACIONALIDADE": "integer"
        },
        "PK": [
            "SQ_CANDIDATO"
        ],
        "FK": {
            "CD_GENERO": ("genero", "CD_GENERO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_GRAU_INSTRUCAO": ("grau_instrucao", "CD_GRAU_INSTRUCAO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_ESTADO_CIVIL": ("estado_civil", "CD_ESTADO_CIVIL", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_COR_RACA": ("raca", "CD_COR_RACA", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_OCUPACAO": ("ocupacao", "CD_OCUPACAO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_NACIONALIDADE": ("nacionalidade", "CD_NACIONALIDADE", "ON UPDATE CASCADE ON DELETE RESTRICT")
        }
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['SQ_CANDIDATO', 'NR_CANDIDATO', 'NM_CANDIDATO', 'NM_URNA_CANDIDATO', 'NM_SOCIAL_CANDIDATO',
                       'NR_CPF_CANDIDATO', 'NM_EMAIL', 'DT_NASCIMENTO', 'SG_UF_NASCIMENTO', 'CD_MUNICIPIO_NASCIMENTO',
                       'NM_MUNICIPIO_NASCIMENTO', 'NR_TITULO_ELEITORAL_CANDIDATO', 'CD_GENERO', 'CD_GRAU_INSTRUCAO',
                       'CD_ESTADO_CIVIL', 'CD_COR_RACA', 'CD_OCUPACAO', 'CD_NACIONALIDADE']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def candidatura(show_table=False, delete_if_exists=False):
    table_name = "candidatura"
    tab_cols = {
        "cols": {
            "SQ_CANDIDATO": "bigint",
            "CD_ELEICAO": "integer",
            "TP_AGREMIACAO": "varchar(32)",
            "VR_DESPESA_MAX_CAMPANHA": "money",
            "ST_REELEICAO": "varchar(3)",
            "ST_DECLARAR_BENS": "varchar(3)",
            "NR_PROTOCOLO_CANDIDATURA": "bigint",
            "NR_PROCESSO": "bigint",
            "ST_CANDIDATO_INSERIDO_URNA": "varchar(3)",
            "CD_SITUACAO_CANDIDATURA": "integer",
            "CD_DETALHE_SITUACAO_CAND": "integer",
            "CD_CARGO": "integer",
            "NR_PARTIDO": "integer",
            "SQ_COLIGACAO": "bigint",
            "CD_SIT_TOT_TURNO": "integer",
            "CD_SITUACAO_CANDIDATO_PLEITO": "integer",
            "CD_SITUACAO_CANDIDATO_URNA": "integer",
            "SG_UE": "varchar(32)"
        },
        "PK": [
            "SQ_CANDIDATO",
            "CD_ELEICAO"
        ],
        "FK": {
            "SQ_CANDIDATO": ("candidato", "SQ_CANDIDATO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_ELEICAO": ("eleicao", "CD_ELEICAO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_SITUACAO_CANDIDATURA": (
                "situacao_candidatura", "CD_SITUACAO_CANDIDATURA", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_DETALHE_SITUACAO_CAND": (
                "situacao_candidato", "CD_DETALHE_SITUACAO_CAND", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_CARGO": ("cargo", "CD_CARGO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "NR_PARTIDO": ("partido", "NR_PARTIDO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "SQ_COLIGACAO": ("coligacao", "SQ_COLIGACAO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_SIT_TOT_TURNO": ("totalizacao_votos", "CD_SIT_TOT_TURNO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_SITUACAO_CANDIDATO_PLEITO": (
                "situacao_pleito", "CD_SITUACAO_CANDIDATO_PLEITO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "CD_SITUACAO_CANDIDATO_URNA": (
                "situacao_urna", "CD_SITUACAO_CANDIDATO_URNA", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "SG_UE": ("unidade_eleitoral", "SG_UE", "ON UPDATE CASCADE ON DELETE RESTRICT")
        }
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['SQ_CANDIDATO', 'CD_ELEICAO', 'TP_AGREMIACAO', 'VR_DESPESA_MAX_CAMPANHA', 'ST_REELEICAO',
                       'ST_DECLARAR_BENS', 'NR_PROTOCOLO_CANDIDATURA', 'NR_PROCESSO', 'ST_CANDIDATO_INSERIDO_URNA',
                       'CD_SITUACAO_CANDIDATURA', 'CD_DETALHE_SITUACAO_CAND', 'CD_CARGO', 'NR_PARTIDO', 'SQ_COLIGACAO',
                       'CD_SIT_TOT_TURNO', 'CD_SITUACAO_CANDIDATO_PLEITO', 'CD_SITUACAO_CANDIDATO_URNA', 'SG_UE']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def cargo(show_table=False, delete_if_exists=False):
    table_name = "cargo"
    tab_cols = {
        "cols": {
            "CD_CARGO": "integer",
            "DS_CARGO": "varchar(64)"
        },
        "PK": [
            "CD_CARGO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb1 = df_candidato[['CD_CARGO', 'DS_CARGO']]
    tb1 = tb1.drop_duplicates()
    tb2 = df_coligacao[['CD_CARGO', 'DS_CARGO']]
    tb2 = tb2.drop_duplicates()
    tb3 = df_vagas[['CD_CARGO', 'DS_CARGO']]
    tb3 = tb3.drop_duplicates()
    tb = pd.concat([tb1, tb2, tb3], ignore_index=True)
    tb['DS_CARGO'] = tb['DS_CARGO'].apply(lambda x: x.upper())
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def cassacao(show_table=False, delete_if_exists=False):
    table_name = "cassacao"
    tab_cols = {
        "cols": {
            "SQ_CANDIDATO": "bigint",
            "CD_ELEICAO": "integer",
            "DS_MOTIVO_CASSACAO": "varchar(128)"
        },
        "PK": [
            "SQ_CANDIDATO",
            "CD_ELEICAO",
            "DS_MOTIVO_CASSACAO"
        ],
        "FK": {
            "CD_ELEICAO": ("eleicao", "CD_ELEICAO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
        }
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_cassacao[['SQ_CANDIDATO', 'CD_ELEICAO', 'DS_MOTIVO_CASSACAO']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def coligacao(show_table=False, delete_if_exists=False):
    table_name = "coligacao"
    tab_cols = {
        "cols": {
            "SQ_COLIGACAO": "bigint",
            "NM_COLIGACAO": "varchar(128)",
            "DS_COMPOSICAO_COLIGACAO": "varchar(255)"
        },
        "PK": [
            "SQ_COLIGACAO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['SQ_COLIGACAO', 'NM_COLIGACAO', 'DS_COMPOSICAO_COLIGACAO']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def eleicao(show_table=False, delete_if_exists=False):
    table_name = "eleicao"
    tab_cols = {
        "cols": {
            "CD_ELEICAO": "integer",
            "DS_ELEICAO": "varchar(128)",
            "DT_ELEICAO": "date",
            "ANO_ELEICAO": "varchar(8)",
            "NR_TURNO": "smallint",
            "TP_ABRANGENCIA": "varchar(32)",
            "CD_TIPO_ELEICAO": "integer"
        },
        "PK": [
            "CD_ELEICAO"
        ],
        "FK": {
            "CD_TIPO_ELEICAO": ("tipo_eleicao", "CD_TIPO_ELEICAO", "ON UPDATE CASCADE ON DELETE RESTRICT")
        }
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb1 = df_candidato[['CD_ELEICAO', 'DS_ELEICAO', 'DT_ELEICAO', 'ANO_ELEICAO', 'NR_TURNO', 'TP_ABRANGENCIA',
                        'CD_TIPO_ELEICAO']].drop_duplicates()
    tb2 = df_coligacao[
        ['CD_ELEICAO', 'DS_ELEICAO', 'DT_ELEICAO', 'ANO_ELEICAO', 'NR_TURNO', 'CD_TIPO_ELEICAO']].drop_duplicates()
    tb3 = df_bem[['CD_ELEICAO', 'DS_ELEICAO', 'DT_ELEICAO', 'ANO_ELEICAO', 'CD_TIPO_ELEICAO']].drop_duplicates()
    tb4 = df_vagas[['CD_ELEICAO', 'DS_ELEICAO', 'DT_ELEICAO', 'ANO_ELEICAO', 'CD_TIPO_ELEICAO']].drop_duplicates()
    tb5 = df_cassacao[['CD_ELEICAO', 'DS_ELEICAO', 'ANO_ELEICAO', 'CD_TIPO_ELEICAO']].drop_duplicates()

    tb = pd.merge(tb1, tb2, how="outer",
                  on=['CD_ELEICAO', 'DS_ELEICAO', 'DT_ELEICAO', 'ANO_ELEICAO', 'NR_TURNO', 'CD_TIPO_ELEICAO'])
    tb = pd.merge(tb, tb3, how="outer", on=['CD_ELEICAO', 'DS_ELEICAO', 'DT_ELEICAO', 'ANO_ELEICAO', 'CD_TIPO_ELEICAO'])
    tb = pd.merge(tb, tb4, how="outer", on=['CD_ELEICAO', 'DS_ELEICAO', 'DT_ELEICAO', 'ANO_ELEICAO', 'CD_TIPO_ELEICAO'])
    tb = pd.merge(tb, tb5, how="outer", on=['CD_ELEICAO', 'DS_ELEICAO', 'ANO_ELEICAO', 'CD_TIPO_ELEICAO'])

    # tb['NR_TURNO'] = pd.to_numeric(tb['NR_TURNO'], errors='coerce')
    tb['NR_TURNO'] = tb['NR_TURNO'].fillna(-1)
    tb['TP_ABRANGENCIA'] = tb['TP_ABRANGENCIA'].fillna("#NULO")

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def estado_civil(show_table=False, delete_if_exists=False):
    table_name = "estado_civil"
    tab_cols = {
        "cols": {
            "CD_ESTADO_CIVIL": "integer",
            "DS_ESTADO_CIVIL": "varchar(64)"
        },
        "PK": [
            "CD_ESTADO_CIVIL"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_ESTADO_CIVIL', 'DS_ESTADO_CIVIL']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def genero(show_table=False, delete_if_exists=False):
    table_name = "genero"
    tab_cols = {
        "cols": {
            "CD_GENERO": "integer",
            "DS_GENERO": "varchar(32)"
        },
        "PK": [
            "CD_GENERO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_GENERO', 'DS_GENERO']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def grau_instrucao(show_table=False, delete_if_exists=False):
    table_name = "grau_instrucao"
    tab_cols = {
        "cols": {
            "CD_GRAU_INSTRUCAO": "integer",
            "DS_GRAU_INSTRUCAO": "varchar(64)"
        },
        "PK": [
            "CD_GRAU_INSTRUCAO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_GRAU_INSTRUCAO', 'DS_GRAU_INSTRUCAO']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def nacionalidade(show_table=False, delete_if_exists=False):
    table_name = "nacionalidade"
    tab_cols = {
        "cols": {
            "CD_NACIONALIDADE": "integer",
            "DS_NACIONALIDADE": "varchar(64)"
        },
        "PK": [
            "CD_NACIONALIDADE"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_NACIONALIDADE', 'DS_NACIONALIDADE']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def ocorreu(show_table=False, delete_if_exists=False):
    table_name = "ocorreu"
    tab_cols = {
        "cols": {
            "CD_ELEICAO": "integer",
            "SG_UF": "varchar(2)"
        },
        "PK": [
            "CD_ELEICAO",
            "SG_UF"
        ],
        "FK": {
            "CD_ELEICAO": ("eleicao", "CD_ELEICAO", "ON UPDATE CASCADE ON DELETE RESTRICT")
        }
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb1 = df_candidato[['CD_ELEICAO', 'SG_UF']].drop_duplicates()
    tb2 = df_coligacao[['CD_ELEICAO', 'SG_UF']].drop_duplicates()
    tb3 = df_bem[['CD_ELEICAO', 'SG_UF']].drop_duplicates()
    tb4 = df_vagas[['CD_ELEICAO', 'SG_UF']].drop_duplicates()
    tb5 = df_cassacao[['CD_ELEICAO', 'SG_UF']].drop_duplicates()

    tb = pd.merge(tb1, tb2, how="outer", on=['CD_ELEICAO', 'SG_UF'])
    tb = pd.merge(tb, tb3, how="outer", on=['CD_ELEICAO', 'SG_UF'])
    tb = pd.merge(tb, tb4, how="outer", on=['CD_ELEICAO', 'SG_UF'])
    tb = pd.merge(tb, tb5, how="outer", on=['CD_ELEICAO', 'SG_UF'])

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def ocupacao(show_table=False, delete_if_exists=False):
    table_name = "ocupacao"
    tab_cols = {
        "cols": {
            "CD_OCUPACAO": "integer",
            "DS_OCUPACAO": "varchar(128)"
        },
        "PK": [
            "CD_OCUPACAO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_OCUPACAO', 'DS_OCUPACAO']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def partido(show_table=False, delete_if_exists=False):
    table_name = "partido"
    tab_cols = {
        "cols": {
            "NR_PARTIDO": "integer",
            "SG_PARTIDO": "varchar(32)",
            "NM_PARTIDO": "varchar(128)"
        },
        "PK": [
            "NR_PARTIDO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def raca(show_table=False, delete_if_exists=False):
    table_name = "raca"
    tab_cols = {
        "cols": {
            "CD_COR_RACA": "integer",
            "DS_COR_RACA": "varchar(32)"
        },
        "PK": [
            "CD_COR_RACA"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_COR_RACA', 'DS_COR_RACA']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def situacao_candidato(show_table=False, delete_if_exists=False):
    table_name = "situacao_candidato"
    tab_cols = {
        "cols": {
            "CD_DETALHE_SITUACAO_CAND": "integer",
            "DS_DETALHE_SITUACAO_CAND": "varchar(64)"
        },
        "PK": [
            "CD_DETALHE_SITUACAO_CAND"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_DETALHE_SITUACAO_CAND', 'DS_DETALHE_SITUACAO_CAND']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def situacao_candidatura(show_table=False, delete_if_exists=False):
    table_name = "situacao_candidatura"
    tab_cols = {
        "cols": {
            "CD_SITUACAO_CANDIDATURA": "integer",
            "DS_SITUACAO_CANDIDATURA": "varchar(32)"
        },
        "PK": [
            "CD_SITUACAO_CANDIDATURA"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_SITUACAO_CANDIDATURA', 'DS_SITUACAO_CANDIDATURA']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def situacao_pleito(show_table=False, delete_if_exists=False):
    table_name = "situacao_pleito"
    tab_cols = {
        "cols": {
            "CD_SITUACAO_CANDIDATO_PLEITO": "integer",
            "DS_SITUACAO_CANDIDATO_PLEITO": "varchar(64)"
        },
        "PK": [
            "CD_SITUACAO_CANDIDATO_PLEITO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_SITUACAO_CANDIDATO_PLEITO', 'DS_SITUACAO_CANDIDATO_PLEITO']]

    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def situacao_urna(show_table=False, delete_if_exists=False):
    table_name = "situacao_urna"
    tab_cols = {
        "cols": {
            "CD_SITUACAO_CANDIDATO_URNA": "integer",
            "DS_SITUACAO_CANDIDATO_URNA": "varchar(64)"
        },
        "PK": [
            "CD_SITUACAO_CANDIDATO_URNA"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_SITUACAO_CANDIDATO_URNA', 'DS_SITUACAO_CANDIDATO_URNA']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def tipo_bem(show_table=False, delete_if_exists=False):
    table_name = "tipo_bem"
    tab_cols = {
        "cols": {
            "CD_TIPO_BEM_CANDIDATO": "integer",
            "DS_TIPO_BEM_CANDIDATO": "varchar(128)"
        },
        "PK": [
            "CD_TIPO_BEM_CANDIDATO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_bem[['CD_TIPO_BEM_CANDIDATO', 'DS_TIPO_BEM_CANDIDATO']]

    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def tipo_eleicao(show_table=False, delete_if_exists=False):
    table_name = "tipo_eleicao"
    tab_cols = {
        "cols": {
            "CD_TIPO_ELEICAO": "integer",
            "NM_TIPO_ELEICAO": "varchar(64)"
        },
        "PK": [
            "CD_TIPO_ELEICAO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb1 = df_bem[['CD_TIPO_ELEICAO', 'NM_TIPO_ELEICAO']]
    tb1 = tb1.drop_duplicates()
    tb2 = df_candidato[['CD_TIPO_ELEICAO', 'NM_TIPO_ELEICAO']]
    tb2 = tb2.drop_duplicates()
    tb3 = df_cassacao[['CD_TIPO_ELEICAO', 'NM_TIPO_ELEICAO']]
    tb3 = tb3.drop_duplicates()
    tb4 = df_coligacao[['CD_TIPO_ELEICAO', 'NM_TIPO_ELEICAO']]
    tb4 = tb4.drop_duplicates()
    tb5 = df_vagas[['CD_TIPO_ELEICAO', 'NM_TIPO_ELEICAO']]
    tb5 = tb5.drop_duplicates()
    tb = pd.concat([tb1, tb2, tb3, tb4, tb5], ignore_index=True)
    tb['NM_TIPO_ELEICAO'] = tb['NM_TIPO_ELEICAO'].apply(lambda x: x.upper())
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def totalizacao_votos(show_table=False, delete_if_exists=False):
    table_name = "totalizacao_votos"
    tab_cols = {
        "cols": {
            "CD_SIT_TOT_TURNO": "integer",
            "DS_SIT_TOT_TURNO": "varchar(64)"
        },
        "PK": [
            "CD_SIT_TOT_TURNO"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_candidato[['CD_SIT_TOT_TURNO', 'DS_SIT_TOT_TURNO']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def unidade_eleitoral(show_table=False, delete_if_exists=False):
    table_name = "unidade_eleitoral"
    tab_cols = {
        "cols": {
            "SG_UE": "varchar(32)",
            "NM_UE": "varchar(128)"
        },
        "PK": [
            "SG_UE"
        ]
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb1 = df_bem[['SG_UE', 'NM_UE']]
    tb1 = tb1.drop_duplicates()
    tb2 = df_candidato[['SG_UE', 'NM_UE']]
    tb2 = tb2.drop_duplicates()
    tb3 = df_cassacao[['SG_UE', 'NM_UE']]
    tb3 = tb3.drop_duplicates()
    tb4 = df_coligacao[['SG_UE', 'NM_UE']]
    tb4 = tb4.drop_duplicates()
    tb5 = df_vagas[['SG_UE', 'NM_UE']]
    tb5 = tb5.drop_duplicates()
    tb = pd.concat([tb1, tb2, tb3, tb4, tb5], ignore_index=True)
    tb['NM_UE'] = tb['NM_UE'].apply(lambda x: x.upper().replace("\'", " "))
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


def vagas(show_table=False, delete_if_exists=False):
    table_name = "vagas"
    tab_cols = {
        "cols": {
            "CD_CARGO": "integer",
            "CD_ELEICAO": "integer",
            "SG_UE": "varchar(32)",
            "QT_VAGAS": "smallint",
            "DT_POSSE": "date"
        },
        "PK": [
            "CD_CARGO",
            "CD_ELEICAO",
            "SG_UE"
        ],
        "FK": {
            "CD_ELEICAO": ("eleicao", "CD_ELEICAO", "ON UPDATE CASCADE ON DELETE RESTRICT"),
            "SG_UE": ("unidade_eleitoral", "SG_UE", "ON UPDATE CASCADE ON DELETE RESTRICT")
        }
    }

    # CRIAR A TABELA
    crud = Crud(con, tab_cols, table_name)
    if crud.exists()[0][0]:
        if delete_if_exists:
            crud.drop_table()
            crud.create()
    else:
        crud.create()

    # PREPARAR DADOS
    tb = df_vagas[['CD_CARGO', 'CD_ELEICAO', 'SG_UE', 'QT_VAGAS', 'DT_POSSE']]
    tb = tb.drop_duplicates()

    # INSERIR OS DADOS
    crud.insert_lote(tb)

    if show_table:
        tb = crud.busca_todos()
        tb = pd.DataFrame(tb, columns=tab_cols["cols"].keys())
        print(tb)
        print("\n")

    return crud


# INFELIZMENTE NÃO FUNCIONOU
def set_data_time():
    con.execute("SET datestyle TO \"ISO, DMY\";")
    con.commit()


# CONFIGURAÇÕES DO PANDAS PARA VISUALIZAÇÃO NO TERMINAL
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 1000)


def main():
    # INFELIZMENTE NÃO FUNCIONOU, PRECISA SER MANUAL
    set_data_time()

    # OPÇÕES DA EXECUÇÃO
    show = config['show_tables']
    drop = config['drop_if_exists']

    cargo(show, drop)
    coligacao(show, drop)
    estado_civil(show, drop)
    genero(show, drop)
    grau_instrucao(show, drop)
    nacionalidade(show, drop)
    ocupacao(show, drop)
    partido(show, drop)
    raca(show, drop)
    situacao_candidato(show, drop)
    situacao_candidatura(show, drop)
    situacao_pleito(show, drop)
    situacao_urna(show, drop)
    tipo_bem(show, drop)
    tipo_eleicao(show, drop)
    totalizacao_votos(show, drop)
    unidade_eleitoral(show, drop)

    # DEVE EXECUTAR EM ORDEM:
    candidato(show, drop)    # Bem Lento 557.553
    eleicao(show, drop)
    ocorreu(show, drop)
    vagas(show, drop)
    cassacao(show, drop)
    candidatura(show, drop)  # Bem Lento 557.785
    bens(show, drop)         # MUITO Lento 1.012.735


if __name__ == '__main__':
    """  CONFIGURAÇÃO, CSVs E A CONEXÃO FICAM EM ESCOPO GLOBAL  """

    with open("./config.json", 'r') as json_file:
        config = json.load(json_file)

    print(" ------------------------------------------------------------------------------")
    print("|  POR FAVOR CONFIRME QUE SEU BANCO ESTA CONFIGURADO COM O FORMATO DE DATA DMY |")
    print("|                        SET datestyle TO \"ISO, DMY\";                          |")
    print(" ------------------------------------------------------------------------------")

    input("Press Enter to continue...")

    # -------------------------------------------------------------------------------------------------------
    # LOAD DOS CSVs
    print("Loading cvs...", end="")
    tabelas = config['diretorio_tabelas']
    print("\rLoading cvs...(1/5)", end="")
    df_bem = pd.read_csv(f"{tabelas['bem_candidato']}", sep=";", encoding="ISO-8859-1")
    print("\rLoading cvs...(2/5)", end="")
    df_candidato = pd.read_csv(f"{tabelas['consulta_cand']}", sep=";", encoding="ISO-8859-1")
    print("\rLoading cvs...(3/5)", end="")
    df_cassacao = pd.read_csv(f"{tabelas['motivo_cassacao']}", sep=";", encoding="ISO-8859-1")
    print("\rLoading cvs...(4/5)", end="")
    df_coligacao = pd.read_csv(f"{tabelas['consulta_coligacao']}", sep=";", encoding="ISO-8859-1")
    print("\rLoading cvs...(5/5)", end="")
    df_vagas = pd.read_csv(f"{tabelas['consulta_vagas']}", sep=";", encoding="ISO-8859-1")
    print("\rLoading cvs...Done")

    # CRIA A CONEXÃO COM O BANCO
    print("Creating PostgreSql conection...", end="")
    con = Connection(config['postgres'])
    print("Done\n")

    main()
