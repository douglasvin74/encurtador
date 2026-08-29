# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Projeto

Encurtador de URLs de estudo, construído em fases. Cada fase adiciona **um**
conceito novo e só avança quando a anterior está rodando. Documentação em
português: `DOC-TECNICO.md` (roadmap das 8 fases + stack) e `DOC-SIMPLES.md`
(mesmo conteúdo sem jargão, com dicionário). Leia o roadmap antes de adicionar
qualquer coisa — o que parece faltando geralmente é assunto de uma fase futura.

Código e comentários em português, sem acentos no `.java`.

## Rodar

```bash
cd fase1 && java -ea Encurtador.java   # fase 1
cd fase2 && java -ea Encurtador.java   # fase 2
cd fase3 && java -ea Encurtador.java   # fase 3
cd fase4 && java -ea Encurtador.java   # fase 4 - servidor em :8080, arg opcional muda a porta

# fases 5+: Maven + Kafka
~/.local/opt/kafka/bin/kafka-server-start.sh -daemon ~/.local/opt/kafka/config/server.properties
cd fase5 && mvn -q exec:exec [-Dporta=9090]
cd fase6 && mvn -q exec:exec                             # escrita, :8080
cd fase6 && mvn -q exec:exec -Dclasse=Contador -Dporta=8081  # leitura, :8081

# fase 7: + Postgres + Nginx na maquina. Exige DB_PASSWORD no ambiente.
cd fase7 && INSTANCIA=a mvn -q exec:exec -Dporta=8091
cd fase7 && INSTANCIA=b mvn -q exec:exec -Dporta=8092
cd fase7 && mvn -q exec:exec -Dclasse=Contador -Dporta=8081
mkdir -p /tmp/nginx-encurtador/logs
nginx -p /tmp/nginx-encurtador -c $PWD/fase7/nginx.conf   # :8080 -> 8091, 8092
nginx -p /tmp/nginx-encurtador -s stop

# fase 8 (atual): tudo em containers, nada instalado na maquina
cd fase8 && docker compose up --build     # :8080 escrita, :8081 leitura
cd fase8 && docker compose down           # -v tambem apaga o volume do banco
```

`pgrep -f kafka.Kafka` casa com o proprio comando e mente que o broker esta no
ar; use `ss -lptn 'sport = :9092'` ou tente conectar.

Cada fase é um diretório próprio com um `Encurtador.java` autocontido. Fases
anteriores não são apagadas nem refatoradas — servem de comparação.

Fases 1–4: *single-file source launcher*, sem build tool. Fase 5 em diante:
Maven, porque `kafka-clients` é a primeira dependência fora da stdlib.

`exec:exec` (não `exec:java`) para conseguir passar `-ea`. A porta é property do
pom, não `-Dexec.args` — esse último substitui a lista inteira de argumentos.

Kafka 4.3.1 KRaft em `~/.local/opt/kafka` (sem Docker até a fase 8);
`KAFKA_BOOTSTRAP` sobrescreve `localhost:9092`.

Na fase 8 nada disso precisa estar instalado: Postgres, Kafka e Nginx viram
serviços do compose, e o código Java não mudou uma linha entre a 7 e a 8 —
só os valores de ambiente. O volume do Postgres 18 monta em
`/var/lib/postgresql`, não no subdiretório `data` (a imagem aborta o boot).

Postgres 18 na fase 7, base `encurtador`. Pelo socket o `psql` usa `peer` e não
pede senha; o JDBC vai por TCP e cai no `scram-sha-256` — o role precisa de
senha e `DB_PASSWORD` precisa estar no ambiente, senão nada sobe. Config toda
por env: `DB_URL`, `DB_USER`, `DB_PASSWORD`, `KAFKA_BOOTSTRAP`, `INSTANCIA`.

`-ea` é obrigatório — os testes são `assert` dentro de `autoTeste()`, e sem a
flag eles passam silenciosamente sem testar nada. Não há framework de teste;
JUnit chega junto com o Maven.

## Regras que a estrutura implica

- **Não antecipe fases.** Limitações da fase atual são deliberadas e estão
  documentadas: `HashMap` não thread-safe (vira `ConcurrentHashMap` na fase 4),
  mesma URL gerando códigos diferentes (hash determinístico na fase 2), estado
  só em memória (persistência na fase 2).
- **Sem framework web.** Nada de Spring — a fase 3 usa `jdk.httpserver` de
  propósito, para o mecanismo ficar visível.
- **Concorrência (fase 4) são três mudanças acopladas**: executor de virtual
  threads + `ConcurrentHashMap` + `putIfAbsent` no lugar de check-then-act.
  Mexer numa sem as outras produz perda silenciosa de dados. `salvar()` é
  `synchronized` porque o arquivo não é concorrente.
- **Validação na fronteira**, antes de qualquer escrita: rejeita null/blank e,
  desde a fase 3 (URL vem da rede), qualquer esquema que não seja http/https —
  senão vira open redirect. `IllegalArgumentException` → HTTP 400.
- **O clique nunca espera o Kafka** (fase 5): publicação é assíncrona e sai
  depois da resposta HTTP; falha só loga. Broker fora do ar = estatística
  perdida, não site fora do ar. `max.block.ms` curto existe por isso.
- **302, não 301, a partir da fase 5**: 301 é cacheado e o clique deixa de
  passar pelo servidor — não haveria o que contar.
- **`Encurtador` e `Contador` não se chamam** (fase 6, CQRS). A única ligação é
  o tópico. Não crie dependência direta entre os dois lados; o teste fim a fim
  é o que prova a ligação.
- **`commitSync()` depois de processar** o lote, nunca antes: at-least-once.
  Contar duas vezes é aceitável para estatística; perder não é.
- **Nada de estado no processo a partir da fase 7.** O mapa código→URL mora no
  Postgres, e é isso que permite N instâncias. Qualquer cache local que sobreviva
  entre requisições reintroduz o 404 de "encurtei em A, cliquei em B" —
  `AutoTeste.duasInstancias()` existe para pegar exatamente isso.
- **`/health` pergunta pelo banco, nunca pelo Kafka.** Sem banco a instância não
  atende e deve sair da rotação; sem Kafka ela atende normal. Reprovar por causa
  do broker tiraria todas as instâncias juntas e derrubaria o site para proteger
  um contador.
- **`ON CONFLICT DO NOTHING RETURNING`** é o `putIfAbsent` da fase 4 no banco:
  quem arbitra a corrida entre processos é a chave primária. Trocar por
  `SELECT` seguido de `INSERT` traz o check-then-act de volta.
- **Teste não compartilha estado global.** As instâncias sobem juntas no
  compose e rodam o auto-teste contra a mesma tabela e o mesmo tópico. Por isso
  cada execução tem sua marca de URL (`MARCA`) e seu `group.id` — apagar ou
  consumir "tudo" faz o teste de uma instância derrubar o da outra, e o sintoma
  não se parece nada com a causa.
- Toda lógica nova ganha uma asserção em `autoTeste()`.
