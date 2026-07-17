# Como Rodar
Iniciar instância Airflow:
```shell 
astro dev start
```
Dependências
Pastas `./dbt` e `./include/local_setup` são submódulos. Após commit e push nos repos originais, para trazer códigos atualizados executar:
```shell
# 1. Atualiza o repo principal
git pull origin main

# 2. Puxa o commit mais recente de cada submódulo (branch de tracking deles)
git submodule update --remote

# 3. Registra os novos ponteiros no repo principal
git add <submódulos que mudaram>
git commit -m "chore: bump submódulos para última versão"
git push origin main
```

# Troubleshooting
Problema:
Erro ao criar tabela via dataframe com pandas `to_sql`: "Engine object has no attribute 'cursor' "
Solução:
Downgrade da versão do pandas para 2.1.4 para funcionar com sqlalchemy 1.4.54
