# Encurtador de URLs — Documentação Técnica

Projeto de estudo, construído em fases. Cada fase adiciona **um** conceito novo
e só avança quando a anterior está rodando.

## Roadmap

| Fase | Entrega | Conceito introduzido |
|------|---------|----------------------|
| 1 ✅ | Programa de terminal, estado em memória | Classe, método, tipo, `Map`, ponto de entrada `main` |
| 2 ✅ | Persistência e idempotência | Hash determinístico (mesma URL → mesmo código), SHA-256, I/O de arquivo |
| 3 ✅ | Servidor HTTP (`jdk.httpserver`) | Requisição/resposta, roteamento, códigos de status, `HTTP 301` |
| 4 ✅ | Concorrência com virtual threads | `Executors.newVirtualThreadPerTaskExecutor()`, custo de thread, bloqueio de I/O, corrida check-then-act |
| 5 ✅ | Publicação de eventos de clique no Kafka | Produtor, tópico, partição, chave, entrega assíncrona, degradação graciosa |
| 6 ✅ | Consumidor de estatísticas | Grupo de consumidores, offset, rebalanceamento, at-least-once, CQRS |
| 7 | Múltiplas instâncias atrás de load balancer | Statelessness, round-robin, health check, escala horizontal |
| 8 | Empacotamento | Docker, `docker compose`, variáveis de ambiente |

## Stack

- **Java 25 (LTS)** — virtual threads finalizadas desde a 21; escolhido pelo suporte de longo prazo.
- **Maven** — build e dependências. Adiado: até a fase 4 nada sai da stdlib, então o arquivo único continua bastando. Entra quando houver dependência de verdade (Kafka, fase 5).
- **Kafka 4.3.1 (KRaft)** — broker de eventos. Instalado em `~/.local/opt/kafka`, rodando na JVM local; sem Docker até a fase 8.
- **Nginx** — load balancer (fase 7).
- Sem framework web (nada de Spring) por escolha: o objetivo é ver o mecanismo,
  não a abstração que o esconde.

## Fase 1 — concluída

Arquivo único: `fase1/Encurtador.java`. Roda pelo *single-file source launcher*
do Java (`java Arquivo.java`), sem passo de compilação separado e sem build tool.

### Modelo

```
Map<String, String> links   // código curto -> URL original
```

`HashMap` dá busca em tempo médio O(1). Não é thread-safe — irrelevante na fase 1
porque só existe uma thread; vira problema explícito na fase 4, e é lá que
trocamos por `ConcurrentHashMap`.

### Geração de código

6 caracteres de um alfabeto de 31 símbolos (dígitos + consoantes) → 31⁶ ≈ 887
milhões de combinações. Sorteio aleatório com verificação de colisão em laço
`do/while`.

Limitação conhecida: a mesma URL encurtada duas vezes gera dois códigos
distintos, desperdiçando espaço de chaves. Resolvido na fase 2 com hash
determinístico da URL.

### Validação

`encurtar` rejeita `null` e string em branco com `IllegalArgumentException`.
Validação na fronteira de entrada, antes de qualquer escrita no mapa.

### Verificação

`autoTeste()` roda dentro do `main` com `assert`. Cobre: tamanho do código,
ida e volta da URL, código inexistente, contagem, unicidade e recusa de entrada
vazia. Sem framework de teste — JUnit entra quando houver Maven (fase 3).

**Importante:** `assert` só é avaliado com a flag `-ea` (*enable assertions*).
Sem ela, o auto-teste passa silenciosamente sem testar nada.

## Como rodar

```bash
cd encurtador/fase1 && java -ea Encurtador.java   # fase 1
cd encurtador/fase2 && java -ea Encurtador.java   # fase 2
cd encurtador/fase3 && java -ea Encurtador.java   # fase 3
cd encurtador/fase4 && java -ea Encurtador.java   # fase 4

# fase 5 (estado atual): Maven, precisa do broker
~/.local/opt/kafka/bin/kafka-server-start.sh -daemon ~/.local/opt/kafka/config/server.properties
cd encurtador/fase5 && mvn -q exec:exec              # porta 8080

# fase 6 (estado atual): dois programas, um de cada lado do Kafka
cd encurtador/fase6 && mvn -q exec:exec                            # escrita, :8080
cd encurtador/fase6 && mvn -q exec:exec -Dclasse=Contador -Dporta=8081  # leitura, :8081
```

Cada fase é um diretório independente e roda sozinha — a anterior continua
funcionando para comparação.

## Fase 2 — concluída

`fase2/Encurtador.java`. Mesmo formato de arquivo único, duas mudanças.

### Idempotência

O código deixou de ser sorteado e passou a ser **derivado** da URL:

```
SHA-256(url + "#" + tentativa) -> 6 primeiros bytes -> 1 caractere cada: (byte & 0xff) % 31
```

Mesma entrada, mesma saída, sempre. Encurtar a mesma URL duas vezes devolve o
mesmo código e não escreve nada — o problema deixado em aberto na fase 1.

O parâmetro `tentativa` só existe para colisão: se o código já está ocupado por
**outra** URL, incrementa e re-hasheia. Se está ocupado pela *mesma* URL, devolve
e encerra. A colisão aqui é do código de 6 chars (31⁶ ≈ 887M), não do SHA-256.

Custo: o código virou função pública da URL. Quem conhece o algoritmo descobre o
código de qualquer URL sem consultar o serviço. Aceitável para encurtador; não
seria para link secreto.

### Persistência

`java.util.Properties` sobre um arquivo `codigo=url`, carregado no construtor e
regravado a cada link novo. Arquivo inexistente é começo do zero, não erro.
Escolhido por ser stdlib e resolver escape de caracteres sozinho.

Limitação assumida (`ponytail:` no código): `salvar()` reescreve o arquivo
inteiro a cada gravação — O(n) por escrita. Serve até alguns milhares de links;
banco de dados quando incomodar.

O `main` grava em `links.properties` no diretório atual (ignorado pelo git); o
auto-teste usa arquivo temporário e apaga no `finally`.

### Verificação

`autoTeste()` cobre o da fase 1 mais: mesma URL → mesmo código sem duplicar
contagem; URLs diferentes → códigos diferentes; e releitura do arquivo por uma
segunda instância devolvendo o mesmo resultado.

## Fase 3 — concluída

`fase3/Encurtador.java`. O domínio das fases 1–2 intacto; em volta dele, um
servidor HTTP.

### Servidor

`com.sun.net.httpserver.HttpServer` (módulo `jdk.httpserver`, já no JDK). Sem
Spring e sem Maven por escolha — requisição, resposta e status ficam visíveis.

`servir(porta)` devolve o `HttpServer` em vez de guardá-lo: quem sobe decide
quando parar. É o que deixa o auto-teste subir na porta 0 (SO escolhe uma livre)
e derrubar no `finally`.

### Rotas

| Método | Caminho | Resposta |
|--------|---------|----------|
| POST | `/encurtar` | `201` + `/codigo` (corpo da requisição = URL em texto puro) |
| GET | `/<codigo>` | `301` + header `Location`, ou `404` |
| GET | `/` | `200` com o modo de uso |
| outros | — | `405` |

`301` (permanente) e não `302`: o navegador cacheia e nas próximas vezes vai
direto ao destino. Tem um efeito colateral que aparece na fase 5 — clique
cacheado não passa mais pelo servidor, logo não é contado.

`400` para URL recusada: erro do cliente, não do servidor.

### Validação virou fronteira de confiança

Da fase 3 em diante a URL chega pela rede. `validar()` passou a exigir esquema
`http` ou `https` — sem isso o serviço redirecionaria para `javascript:...`,
um *open redirect*. Continua na entrada, antes de qualquer escrita.

### Concorrência: ainda não

O executor padrão do `HttpServer` atende **uma** requisição por vez, e o
`HashMap` continua não sendo thread-safe. Os dois problemas são a fase 4.

### Verificação

`autoTeste()` roda antes do `main` subir o servidor: sobe um servidor real em
porta efêmera e conversa com ele via `java.net.http.HttpClient` com
`Redirect.NEVER` (para ver o 301 em vez de segui-lo). Cobre 201, idempotência
pela rede, 301 + `Location`, 404, 400 para vazio e para `javascript:`, e 200 na
raiz.

## Fase 4 — concluída

`fase4/Encurtador.java`. Mesmas rotas, mesmo domínio; o que muda é atender
muitas requisições ao mesmo tempo. **Três mudanças que só funcionam juntas** —
trocar apenas o executor produz corrupção silenciosa.

### 1. Executor

```java
servidor.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
```

Thread de SO custa ~1MB de pilha: alguns milhares e acabou. Virtual thread custa
bytes e, ao bloquear em I/O, desmonta e devolve a *carrier thread* para outra
requisição. Daí a escala.

Medido: 100 requisições de 1s simultâneas em **1,35s**. Com o executor padrão da
fase 3 seriam ~100s. A rota `GET /lento` (dorme 1s) existe só para tornar isso
observável.

### 2. `ConcurrentHashMap`

`HashMap` sob escrita concorrente não é "às vezes lento", é **corrompido** —
entradas perdidas, e em versões antigas laço infinito no resize. Substituído.

### 3. `putIfAbsent` no lugar de check-then-act

```java
String jaGravado = links.putIfAbsent(codigo, urlOriginal);
```

`if (get() == null) put()` é duas operações: duas threads passam pelo `if`
juntas e uma sobrescreve a outra. `putIfAbsent` é um passo atômico — grava e
devolve `null`, ou não grava e devolve o ocupante. Os três desfechos ficam
explícitos: gravamos nós / outra thread já gravou a mesma URL / colisão real.

Trocar o `ConcurrentHashMap` sem trocar isto ainda dá bug: o mapa fica íntegro,
a *lógica* é que corre.

### Escrita em arquivo

`salvar()` virou `synchronized`. O mapa é concorrente; o arquivo não. Sem a
trava, duas threads reescrevendo deixam o arquivo pela metade.

`ponytail:` no código — trava global **e** reescrita integral por gravação. É o
gargalo de escrita assumido desta fase. O redirect (leitura) não passa por
`salvar()` e não trava, que é o caminho quente de um encurtador.

### Verificação

Além do contrato da fase 3, `autoTeste()` agora prova a concorrência:

- 200 URLs distintas gravadas em paralelo → `quantidade() == 201`, nenhuma perdida (falha com `HashMap`);
- 50 threads disputando a **mesma** URL → um único código para todas (falha com check-then-act);
- 50 requisições de 1s em paralelo terminando em menos de 10s (falha com executor serial).

Cada asserção corresponde a uma das três mudanças.

## Fase 5 — concluída

`fase5/`. Primeira fase com **Maven**: `kafka-clients` é a primeira dependência
que não vem da stdlib. Duas classes agora — `Encurtador` e `AutoTeste`, que
cresceu ao precisar consumir do Kafka para verificar.

### Broker

Kafka 4.3.1 em modo **KRaft** (sem ZooKeeper), instalado em `~/.local/opt/kafka`,
logs em `~/.local/var/kafka-logs`. Roda na JVM que já existe — Docker fica para a
fase 8, que é a fase dele.

```bash
# subir
~/.local/opt/kafka/bin/kafka-server-start.sh -daemon ~/.local/opt/kafka/config/server.properties
# ver os eventos
~/.local/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cliques --from-beginning
# parar
~/.local/opt/kafka/bin/kafka-server-stop.sh
```

`KAFKA_BOOTSTRAP` sobrescreve `localhost:9092`.

### O evento

Tópico `cliques`, criado sozinho na primeira publicação. Chave = código, valor =
JSON montado à mão (só escapa `"` e `\`; um JSON de 4 campos não justifica
dependência):

```json
{"codigo":"h7bplw","destino":"https://www.google.com/busca","instante":"2026-08-27T00:25:07.762Z","agente":"curl/8.18.0"}
```

**A chave importa**: o Kafka escolhe a partição por ela, então todos os cliques
de um mesmo código caem na mesma partição e chegam em ordem. É o que permite
somar sem bagunça na fase 6.

### A regra da fase: o clique não espera o Kafka

`send()` é assíncrono e a resposta HTTP sai **antes** da publicação. O callback
de erro só loga. Broker fora do ar = estatística perdida, não site fora do ar.

Config alinhada a isso: `max.block.ms=2000` (send desiste em vez de segurar a
thread do redirect), `acks=1` (é estatística, não dinheiro), `linger.ms=20`
(agrupa envios). `close()` no shutdown hook, para esvaziar a fila pendente.

### 301 → 302

O `301` da fase 3 fazia o navegador decorar o destino e parar de passar pelo
servidor — clique invisível, estatística furada. Trocado por `302` ("por
enquanto é ali"), que traz todo clique de volta. Custo consciente: mais
requisições. Quem não precisa contar deve ficar no 301.

### Verificação

`AutoTeste.rodar()` faz três blocos:

1. **contrato HTTP** — o das fases 3–4, com 302 no lugar de 301;
2. **degradação** — produtor apontado para uma porta morta: o redirect ainda
   responde 302 em menos de 4s (os WARN de conexão no console *são* o teste);
3. **fim a fim** — clica e consome do tópico até achar o evento pela chave.

Sem broker no ar, o bloco 3 é pulado com aviso e o resto continua valendo —
`assert` só, ainda sem JUnit: o que importa aqui é integração (servidor real +
broker real), e o auto-teste roda antes do `main` subir o servidor.

Detalhe que custou um ciclo: o consumidor do teste usa grupo novo com
`auto.offset.reset=earliest`. Com `latest`, entrar no grupo demora alguns
segundos e o evento já passou quando ele chega.

## Fase 6 — estado atual

`fase6/`. Dois programas no mesmo projeto, um de cada lado do Kafka:

| Programa | Papel | Porta |
|----------|-------|-------|
| `Encurtador` | escrita: encurta, redireciona, publica o clique | 8080 |
| `Contador` | leitura: consome os cliques e serve estatísticas | 8081 |

Eles **não se conhecem** — nenhuma chamada direta, nenhuma classe compartilhada
além do nome do tópico. A única ligação é o Kafka. Isso é **CQRS**: o modelo de
escrita (`código → URL`) e o de leitura (`código → contagem`) são estruturas
diferentes, em processos diferentes, escalando de forma independente.

Consequência prática: derrubar o `Contador` não afeta ninguém clicando. Os
eventos ficam no tópico e, quando ele volta, retoma do offset onde parou.

### Rotas do lado da leitura

```
GET /stats           -> [{"codigo":"phn7x9","cliques":7,"ultimo":"2026-08-27T00:34:38.948Z"}]
GET /stats/<codigo>  -> {"codigo":"phn7x9","cliques":7,"ultimo":"..."}
```

Código sem clique responde `"cliques":0`, não 404 — "ninguém clicou" é uma
resposta legítima, e o lado da leitura nem sabe quais códigos existem.

Nada de parsear JSON no consumo: a **chave** do registro já é o código e o
`timestamp` já vem no `ConsumerRecord`.

### At-least-once

`enable.auto.commit=false` e `commitSync()` **depois** de processar o lote:

- commit depois → cair no meio reprocessa o lote → algum clique conta **duas vezes**;
- commit antes → cair no meio **perde** cliques.

Para estatística, contar demais é menos ruim que perder. Quem precisa de
exactly-once paga com transações e idempotência no consumidor — fora do escopo.

### Grupo, partição, rebalanceamento

`group.id=contador-cliques`. Membros do mesmo grupo **dividem** as partições:
cada partição é lida por no máximo um membro. Por isso `prepararTopico()` sobe o
tópico para 3 partições (dá para aumentar, nunca diminuir).

Para ver acontecer, suba um segundo `Contador` em outra porta e acompanhe o log:
o Kafka repassa parte das partições para ele. Derrube um, e o outro reassume —
isso é o *rebalance*.

Limitação assumida (`ponytail:` no código): a contagem vive na memória de cada
instância. Com dois `Contador` no ar, cada um enxerga só as partições que lhe
couberam — logo, contagem parcial. Somar de verdade em N instâncias exige estado
compartilhado (banco, Redis) ou Kafka Streams com store.

### Verificação

`AutoTeste` fecha o circuito: sobe `Encurtador` **e** `Contador` no mesmo teste,
dá 5 cliques de um lado e espera a contagem chegar do outro, com o Kafka no
meio. Depois confere o mesmo número pelo HTTP de `/stats`. É o único jeito
honesto de testar CQRS — as duas pontas não se conhecem, só o comportamento fim
a fim prova que a ligação existe.

A espera é ativa com prazo (20s): consumo é assíncrono, a contagem chega, mas
não instantaneamente.