# Imagem unica para os DOIS programas.
#
# Encurtador e Contador compartilham o mesmo jar e o mesmo classpath; o que
# muda e o comando. Duas imagens quase identicas seriam duas coisas para
# construir, versionar e manter em sincronia - a diferenca entre elas cabe numa
# linha do compose.

# --- build: precisa do Maven e do JDK, que nao vao para a imagem final -------
FROM maven:3-eclipse-temurin-25 AS build
WORKDIR /app

# O pom sozinho primeiro: enquanto as dependencias nao mudam, o Docker reaproveita
# a camada com o .m2 pronto e nao rebaixa a internet a cada alteracao de codigo.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline

COPY src ./src
RUN mvn -q -B package -DskipTests \
 && mvn -q -B dependency:copy-dependencies -DoutputDirectory=target/libs

# --- runtime: so o JRE ------------------------------------------------------
# Imagem final sem Maven, sem JDK, sem codigo-fonte: menor superficie e menos
# coisa para dar errado do que carregar o ambiente de build junto.
FROM eclipse-temurin:25-jre
WORKDIR /app

# Nao rodar como root: se alguem escapar do processo, escapa para um usuario
# sem poder nenhum. O JRE nao precisa de root para abrir porta acima de 1024.
# curl entra so por causa do healthcheck do compose: sem ele nao ha como
# perguntar /health de dentro do container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home encurtador
USER encurtador

COPY --from=build /app/target/classes ./classes
COPY --from=build /app/target/libs ./libs

EXPOSE 8080

# -ea liga o auto-teste, como em todas as fases. CLASSE decide qual programa
# sobe (Encurtador ou Contador) - e a unica diferenca entre os dois servicos.
ENV CLASSE=Encurtador
ENTRYPOINT ["sh", "-c", "exec java -ea -cp 'classes:libs/*' $CLASSE 8080"]
