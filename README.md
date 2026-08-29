# Encurtador de URLs

Encurtador de URLs distribuído: duas instâncias sem estado atrás de um load
balancer, eventos de clique no Kafka e estatísticas servidas por um processo
separado. Java 25, sem framework web.

```bash
git clone https://github.com/douglasvin74/encurtador
cd encurtador
docker compose up --build
```

```bash
curl -X POST --data 'https://exemplo.com/promo' http://localhost:8080/encurtar
# /phn7x9
curl -i http://localhost:8080/phn7x9      # 302 para a URL original
curl http://localhost:8081/stats/phn7x9   # {"codigo":"phn7x9","cliques":1,...}
```

## Arquitetura

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

**Escrita e leitura são processos separados que não se conhecem.** O
`Encurtador` publica cada clique no Kafka; o `Contador` consome e agrega. Não há
chamada direta entre eles — a única ligação é o tópico. Derrubar o contador não
afeta quem está clicando, e os eventos esperam no tópico até ele voltar.

Decisões que sustentam esse desenho:

| Decisão | Por quê |
|---|---|
| Estado no Postgres, nada no processo | É o que torna as instâncias intercambiáveis; sem isso, encurtar numa e clicar na outra daria 404 |
| `ON CONFLICT DO NOTHING RETURNING` | Escrita concorrente entre *processos* — quem arbitra a corrida é a chave primária, não uma estrutura em memória |
| Código = hash da URL | Mesma URL, mesmo código, em qualquer instância. Torna `POST` idempotente e permite `proxy_next_upstream` com segurança |
| Publicação de clique assíncrona | O redirect nunca espera o broker. Kafka fora do ar = estatística perdida, não site fora do ar |
| `302`, não `301` | `301` é cacheado pelo navegador e o clique deixa de passar pelo servidor — não haveria o que contar |
| `/health` consulta o banco, ignora o Kafka | Sem banco a instância não atende e deve sair da rotação; sem Kafka ela atende normal. Reprovar por causa do broker derrubaria todas de uma vez |
| Virtual threads no servidor HTTP | Um thread por requisição sem o custo de thread de sistema |
| `commitSync()` depois de processar | At-least-once: para estatística, contar duas vezes é melhor que perder |

## Rotas

| Método | Rota | Resposta |
|---|---|---|
| `POST` | `/encurtar` | `201` com o código, ou `400` se a URL for inválida |
| `GET` | `/<codigo>` | `302` para a URL original, ou `404` |
| `GET` | `/health` | `200` se o banco responde, `503` se não |
| `GET` | `/stats` *(:8081)* | Todos os códigos com contagem e último clique |
| `GET` | `/stats/<codigo>` *(:8081)* | Um código. Sem cliques responde `0`, não `404` |

## Stack

Java 25 · Maven · PostgreSQL 18 · Kafka 4.3.1 (KRaft) · Nginx · Docker Compose

Sem framework web: o servidor HTTP é o `jdk.httpserver` da própria stdlib.

## Testes

Cada programa carrega um `AutoTeste` com asserções que **rodam no boot** — se
algo estiver quebrado, o container não sobe. Não há framework de teste, e a
flag `-ea` é obrigatória (sem ela os `assert` passam sem testar nada).

Os dois testes que provam o desenho:

- **Duas instâncias** — sobe duas cópias sem referência uma à outra, encurta
  numa e resolve na outra. É a verificação de que não sobrou estado no processo.
- **Circuito completo** — clica de um lado e espera a contagem chegar do outro,
  com o Kafka no meio. É o único jeito honesto de testar dois processos que não
  se conhecem: só o comportamento fim a fim prova que a ligação existe.

Verificado com a stack no ar: round-robin alternando entre as instâncias,
failover com uma delas parada (302 em todas as requisições, nenhum 502), e os
links sobrevivendo a um `docker compose down` seguido de `up`.

## Como foi construído

O projeto nasceu como estudo e foi construído em **oito fases**, cada uma
adicionando um conceito e só avançando quando a anterior estava rodando:

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

Cada fase vivia num diretório próprio e nenhuma era refatorada depois — dava
para abrir a fase 1 e a fase 8 lado a lado e ver o que mudou. Este repositório
guarda apenas o resultado final; **o código de cada fase continua no histórico
do git**, nos commits que as introduziram.

O [DOC-TECNICO.md](DOC-TECNICO.md) documenta as oito fases: o que cada uma
entregou, as decisões tomadas, as limitações assumidas de propósito e por que
cada limitação só foi resolvida na fase seguinte.

Vale registrar uma consequência do método: da fase 7 para a 8 **nenhuma linha de
Java mudou**. Só entraram `Dockerfile`, `compose.yaml` e nomes de serviço no
lugar de portas — porque a fase 7 já tinha tirado toda configuração do código
para variáveis de ambiente. Preparar o terreno numa fase para a seguinte ficar
fácil foi, no fim, do que o projeto se tratou.
