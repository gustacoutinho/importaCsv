class Crud:
    map_type = {
        "integer": int,
        "bigint": int,
        "smallint": int,
        "varchar": str,
        "date": str,
        "time": str,
        "money": float,
    }

    def __init__(self, con, dict_create, table_name, schema_name="public"):
        self.con = con
        self.dict_create = dict_create
        self.table_name = table_name
        self.schema_name = schema_name

    def busca_todos(self):
        try:
            sql = f"SELECT * FROM {self.schema_name}.{self.table_name};"
            v = self.con.query(sql)
            self.con.commit()
        except Exception as e:
            print("Erro ao buscar linhas --", e)
            v = False
        return v

    def create(self):
        try:
            sql = f"CREATE TABLE {self.schema_name}.{self.table_name} ({self.mount_create()});"
            self.con.execute(sql)
            self.con.commit()
        except Exception as e:
            print("Erro ao criar tabela --", e)

    def drop_table(self):
        try:
            sql = f"DROP TABLE {self.schema_name}.{self.table_name};"
            self.con.execute(sql)
            self.con.commit()
        except Exception as e:
            print("Erro ao excluir tabela --", e)

    def exists(self):
        try:
            sql = f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '{self.table_name}');"
            v = self.con.query(sql)
            self.con.commit()
        except Exception as e:
            print("Erro ao excluir linhas --", e)
            v = False
        return v

    def insert(self, *args, coluns=None):
        try:
            s = "%s, "
            n = len(args) - 1
            if coluns is None:
                sql = f"INSERT INTO {self.schema_name}.{self.table_name} ({self.mount_insert()}) VALUES ({s * n}%s);"
            else:
                sql = f"INSERT INTO {self.schema_name}.{self.table_name} ({coluns}) VALUES ({s * n}%s);"
            self.con.execute(sql, args)
            self.con.commit()
        except Exception as e:
            print("Erro ao inserir --", e)

    def insert_lote(self, tb):
        up_to = len(tb)
        padding = " " * (30 - len(self.table_name))

        # Para Cada Linha da Tabela
        for i in range(up_to):
            print(f"\rTABELA {self.table_name.upper()}{padding}...INSERINDO: {i + 1} / {up_to}", end="")
            args = []
            # Para Cada Coluna da Tabela
            for j, val in enumerate(self.dict_create["cols"].values()):
                val = val.split("(")
                args.append(self.map_type[val[0]](tb.iloc[i, j]))

            self.insert(*args)

        padding = " " * (18 - 2*len(str(up_to)))
        print(f"{padding}...DONE")

    def mount_create(self):
        s = ""

        for key, val in self.dict_create["cols"].items():
            s += f"{key} {val}, "

        if "PK" in self.dict_create and len(self.dict_create["PK"]):
            s += "primary key("
            for key in self.dict_create["PK"]:
                s += f"{key}, "
            s = s[:-2] + "), "

        if "FK" in self.dict_create and len(self.dict_create["FK"]):
            for key, val in self.dict_create["FK"].items():
                s += f"FOREIGN KEY ({key}) REFERENCES {val[0]} ({val[1]}) {val[2]}, "
        return s[:-2]

    def mount_insert(self):
        s = ""
        for key in self.dict_create["cols"]:
            s += f"{key}, "
        return s[:-2]
