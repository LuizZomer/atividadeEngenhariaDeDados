## Projeto Apache Spark com Delta Lake

Projeto desenvolvido para demonstração do Apache Spark Local (pyspark) gravando arquivos no formato Delta Lake também de forma local.

Utilize o WSL do Windows 11 ou alguma outra distro Linux. Não utilize ambiente Windows, pois será necessário efetuar várias configurações adicionais para funcionar.

Para este laboratório foi utilizado o Linux Ubuntu 24.04.

![Versao Linux](/assets/linux-version.png)


Projeto python inicializado com o [UV](https://github.com/astral-sh/uv).

Comandos utilizados para setup do ambiente:

```bash copy
uv init
uv venv
source .venv/bin/activate
uv add pyspark==3.5.3 delta-spark==3.2.0 jupyterlab ipykernel
```

Os exemplos de código pyspark/python para instanciar o Spark, bem como criar e manipular uma tabela Delta Lake, está no arquivo `spark-delta-lake.ipynb`.

**Nota:** Antes de executar o arquivo citado acima, não esqueça de selecionar o seu ambiente virtual (.venv) como Kernel do seu jupyter notebook.

![image](https://github.com/user-attachments/assets/9f89a471-ec02-4944-9178-3f79665f74bf)

**Nota 2:** O arquivo `.ipynb` (jupyter notebook) foi executado dentro do VS Code.


Links e referências sobre a compatibilidade da versão do Apache Spark (pyspark) e Delta Lake:

[https://docs.delta.io/latest/releases.html](https://docs.delta.io/latest/releases.html) <br>
[https://mvnrepository.com/artifact/io.delta/delta-spark](https://mvnrepository.com/artifact/io.delta/delta-spark) <br>
[https://github.com/delta-io/delta/releases/](https://github.com/delta-io/delta/releases/) <br>
[https://datawaybr.medium.com/como-sair-do-zero-no-delta-lake-em-apenas-uma-aula-d152688a4cc8](https://datawaybr.medium.com/como-sair-do-zero-no-delta-lake-em-apenas-uma-aula-d152688a4cc8)

