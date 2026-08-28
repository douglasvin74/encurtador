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

# fases 5 e 6: Maven + Kafka
~/.local/opt/kafka/bin/kafka-server-start.sh -daemon ~/.local/opt/kafka/config/server.properties
cd fase5 && mvn -q exec:exec [-Dporta=9090]
cd fase6 && mvn -q exec:exec                             # (atual) escrita, :8080
cd fase6 && mvn -q exec:exec -Dclasse=Contador -Dporta=8081  # leitura, :8081
```

Cada fase é um diretório próprio com um `Encurtador.java` autocontido. Fases
anteriores não são apagadas nem refatoradas — servem de comparação.

Fases 1–4: *single-file source launcher*, sem build tool. Fase 5 em diante:
Maven, porque `kafka-clients` é a primeira dependência fora da stdlib.

`exec:exec` (não `exec:java`) para conseguir passar `-ea`. A porta é property do
pom, não `-Dexec.args` — esse último substitui a lista inteira de argumentos.

Kafka 4.3.1 KRaft em `~/.local/opt/kafka` (sem Docker até a fase 8);
`KAFKA_BOOTSTRAP` sobrescreve `localhost:9092`.

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
- Toda lógica nova ganha uma asserção em `autoTeste()`.
