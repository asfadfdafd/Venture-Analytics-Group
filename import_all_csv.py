import pandas as pd
from sqlalchemy import create_engine
import os

# 1) параметры подключения к твоей БД
USER = "postgres"              # логин от pgAdmin
PASSWORD = "0000"     # <<< сюда твой пароль
HOST = "localhost"
PORT = "5432"
DB_NAME = "postgres"  # или как ты назвал новую БД

engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

# 2) папка, где лежат ВСЕ твои csv
FOLDER = r"/Users/asandauren/Downloads/archive"  # <<< поменяй путь на свой

# 3) какие именно файлы грузим (можешь удалить/добавить)
files_to_import = [
    "ipos.csv",
    "funds.csv",
    "acquisitions.csv",
    "investments.csv",
    "milestones.csv",
    "offices.csv",
    "degrees.csv",
    "people.csv",
    "relationships.csv",
    "objects.csv",
    "funding_rounds.csv",
]

for filename in files_to_import:
    path = os.path.join(FOLDER, filename)
    if not os.path.exists(path):
        print(f"❌ Файл {filename} не найден, пропускаю")
        continue

    table_name = filename.replace(".csv", "")
    print(f"📥 Импортирую {filename} -> таблица {table_name}")

    # читаем csv
    df = pd.read_csv(path)

    # пишем в postgres, если таблица была — заменим
    df.to_sql(table_name, engine, if_exists="replace", index=False)

    print(f"✅ Готово: {table_name} ({len(df)} строк)")

print("🎉 Все файлы обработаны")
