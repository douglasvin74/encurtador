# Encurtador de URLs

Projeto de estudo em Java, construído em **oito fases**. Cada fase adiciona *um*
conceito novo e só avança quando a anterior está rodando — o código das fases
antigas fica no repositório, sem refatoração, para dar para comparar.

O produto é simples de propósito: você dá uma URL longa, recebe um código curto,
e o código te leva de volta. O que interessa é o caminho — de um programa de
terminal com um `HashMap` até seis containers atrás de um load balancer.

```bash
git clone https://github.com/douglasvin74/encurtador
cd encurtador/fase8
docker compose up --build
```

```bash
curl -X POST --data 'https://exemplo.com/promo' http://localhost:8080/encurtar
# /phn7x9
curl -i http://localhost:8080/phn7x9      # 302 para a URL original
curl http://localhost:8081/stats/phn7x9   # {"codigo":"phn7x9","cliques":1,...}
```

## As fases

| Fase | Entrega | Conceito |
|------|---------|----------|
| 1 | Programa de terminal, estado em memória | Classe, método, `Map`, `main` |
| 2 | Persistência e idempotência | Hash determinístico, SHA-256, I/O de arquivo |
| 3 | Servidor HTTP | Requisição/resposta, roteamento, códigos de status |
| 4 | Concorrência | Virtual threads, corrida check-then-act |
| 5 | Eventos de clique no Kafka | Produtor, tópico, partição, degradação graciosa |
| 6 | Consumidor de estatísticas | Grupo, offset, rebalanceamento, at-least-once, CQRS |
| 7 | Múltiplas instâncias + Nginx | Statelessness, round-robin, health check |
| 8 | Empacotamento | Docker, `docker compose`, configuração por ambiente |

Fases 1–4 rodam sem build tool, pelo *single-file source launcher*
(`java -ea Encurtador.java`). Maven entra na fase 5, junto com a primeira
dependência fora da stdlib.

## Arquitetura na fase 8

```
                  :8080
              +----------+
   cliente -->|  Nginx   |
              +----+-----+
                   | round-robin
          +--------+--------+
          v                 v
   encurtador-a        encurtador-b     <- copias identicas, sem estado
          |                 |
          +--------+--------+
                   |                 \
                   v                  v
              PostgreSQL           Kafka  -->  contador  (:8081)
             codigo -> url        cliques      codigo -> contagem
```

Os dois lados **não se conhecem**: o `Encurtador` publica cliques no Kafka e o
`Contador` consome. Nenhuma chamada direta entre eles — só o tópico. Derrubar o
contador não afeta quem está clicando.

## Stack

Java 25 · Maven · Kafka 4.3.1 (KRaft) · PostgreSQL 18 · Nginx · Docker Compose

Sem framework web — a fase 3 usa `jdk.httpserver` de propósito, para o mecanismo
ficar visível em vez de escondido atrás de uma abstração.

## Testes

Não há framework de teste: cada fase carrega um `AutoTeste` com `assert`, que
roda no boot do programa. **A flag `-ea` é obrigatória** — sem ela os asserts
passam silenciosamente sem testar nada.

Os testes que definem as duas últimas fases:

- **fase 7** — sobe duas instâncias sem referência uma à outra, encurta numa e
  resolve na outra. Até a fase 6 isso dava 404, e o 404 era a prova de que o
  servidor guardava estado dentro dele.
- **fase 6** — clica de um lado e espera a contagem chegar do outro, com o Kafka
  no meio. É o único jeito honesto de testar CQRS.

## Documentação

- **[DOC-TECNICO.md](DOC-TECNICO.md)** — roadmap, decisões e o porquê de cada
  uma, fase por fase.
- **[DOC-SIMPLES.md](DOC-SIMPLES.md)** — o mesmo conteúdo sem jargão, com
  dicionário no final.
