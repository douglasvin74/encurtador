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
| 7 ✅ | Múltiplas instâncias atrás de load balancer | Statelessness, round-robin, health check, escala horizontal |
| 8 ✅ | Empacotamento | Docker, `docker compose`, variáveis de ambiente |

## Stack

- **Java 25 (LTS)** — virtual threads finalizadas desde a 21; escolhido pelo suporte de longo prazo.
- **Maven** — build e dependências. Adiado: até a fase 4 nada sai da stdlib, então o arquivo único continua bastando. Entra quando houver dependência de verdade (Kafka, fase 5).
- **Kafka 4.3.1 (KRaft)** — broker de eventos. Em `~/.local/opt/kafka` até a fase 7; imagem `apache/kafka:4.3.1` na fase 8.
- **Nginx** — load balancer (fase 7). Instalado via apt até a fase 7; imagem `nginx:1.27-alpine` na fase 8.
- **PostgreSQL 18** — fonte da verdade do mapa código→URL a partir da fase 7. Entra porque é ele que tira o estado de dentro do processo; sem isso não há statelessness, e sem statelessness não há load balance.
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

## Fase 6 — concluída

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
## Fase 7 — concluída

`fase7/`. Os mesmos dois programas da fase 6, mais duas coisas: o estado saiu do
processo e passou a existir um load balancer na frente.

```
                  :8080
              +----------+
   cliente -->|  Nginx   |
              +----+-----+
                   | round-robin
          +--------+--------+
          v                 v
   Encurtador a        Encurtador b        (:8091, :8092)
          |                 |
          +--------+--------+
                   |                 \
                   v                  v
              PostgreSQL           Kafka  -->  Contador (:8081)
             codigo -> url        cliques      codigo -> contagem
```

### O que exatamente muda

Nada no comportamento visível. Encurtar continua devolvendo o mesmo código para
a mesma URL, clicar continua dando 302, a estatística continua chegando. O que
muda é *onde o link mora* — e é isso que permite existir mais de uma cópia.

| | Fase 6 | Fase 7 |
|---|---|---|
| Mapa código→URL | `ConcurrentHashMap` + `links.properties` local | tabela `links` no Postgres |
| Instâncias possíveis | 1 | N |
| Encurtar em A, clicar em B | 404 | 302 |
| Escrita concorrente | `putIfAbsent` (uma JVM) | `ON CONFLICT DO NOTHING` (todas) |

### Statelessness

O processo não guarda nada entre uma requisição e outra. É a única exigência que
o load balancer faz do nosso lado, e a razão é direta: o Nginx manda a
requisição para qualquer instância, sem saber (nem querer saber) o que aconteceu
antes. Se o link vivesse na memória de quem o criou, metade dos cliques cairia na
instância errada e daria 404.

Vale ver o que *não* foi preciso fazer: nada de sessão pegajosa (`ip_hash`),
nada de sincronizar instância com instância. Estado compartilhado num lugar só,
e as cópias viram descartáveis — dá para matar e subir qualquer uma a qualquer
momento. Escala horizontal é consequência disso, não uma feature à parte.

### `ON CONFLICT DO NOTHING` é o `putIfAbsent` da fase 4

O mesmo problema de check-then-act da fase 4, um nível acima. Lá, duas threads
da mesma JVM podiam ler "não existe" ao mesmo tempo e as duas gravarem; o
`ConcurrentHashMap` resolveu. Aqui são dois *processos*, e nenhuma estrutura de
memória alcança os dois — quem decide passa a ser a chave primária da tabela:

```sql
INSERT INTO links (codigo, url) VALUES (?, ?)
  ON CONFLICT (codigo) DO NOTHING RETURNING codigo
```

`RETURNING` é o que diz se a linha é nossa. Vazio significa que alguém chegou
antes: aí lemos quem está lá para saber se é a mesma URL (mesmo código, hash
determinístico da fase 2) ou uma colisão de hash (próxima tentativa).

### Dependência dura e dependência mole

A fase 7 deixa a assimetria explícita:

| | Postgres | Kafka |
|---|---|---|
| Fora do ar | ninguém é redirecionado | tudo funciona, perde-se estatística |
| `/health` pergunta | sim | não |

Por isso `/health` consulta o banco (`SELECT 1`) e ignora o broker. Um health
check que reprovasse por causa do Kafka tiraria todas as instâncias da rotação
ao mesmo tempo — derrubando o site inteiro para preservar um contador. Health
check deve responder "consigo atender?", não "está tudo perfeito?".

Isso não é teoria: durante o teste desta fase o broker caiu sozinho, e o
redirect continuou respondendo 302 normalmente. Só os cliques daquele intervalo
se perderam, com `clique nao publicado` no log.

### Health check no Nginx aberto é passivo

`max_fails=2 fail_timeout=10s`: o Nginx não pergunta nada a ninguém. Ele repara
que a instância falhou *atendendo tráfego real* e a tira da rotação por 10s.
Checagem ativa (bater em `/health` de tempos em tempos) só existe no Nginx Plus.
No aberto, `/health` serve para o auto-teste e para quem está olhando — e será o
que o Docker vai usar como `healthcheck` na fase 8.

`proxy_next_upstream` inclui `non_idempotent`, o que normalmente é arriscado com
POST. Aqui pode: `POST /encurtar` é idempotente desde a fase 2 — a mesma URL
sempre dá o mesmo código, então repetir não cria nada novo.

### Configuração por ambiente

As instâncias são idênticas; só o ambiente difere. `DB_URL`, `DB_USER`,
`DB_PASSWORD`, `KAFKA_BOOTSTRAP` e `INSTANCIA` (só um rótulo, devolvido no
header `X-Instancia` — sem ele não dá para *ver* o round-robin acontecendo).
Isso já é preparação da fase 8: é assim que o `docker compose` vai diferenciar
as cópias.

Detalhe de ambiente que custa tempo: pelo socket Unix o `psql` usa `peer` e não
pede senha, mas o JDBC vai por TCP e cai no `scram-sha-256`. O role precisa de
senha e o `DB_PASSWORD` precisa estar no ambiente.

### Verificação

`AutoTeste.duasInstancias()` é o teste que define a fase: dois `Encurtador`
independentes, em portas diferentes, sem referência um ao outro. Encurta no
primeiro, resolve no segundo. Até a fase 6 este teste falhava com 404 — e
falhava por um bom motivo.

O que foi verificado à mão, com o Nginx no ar:

- round-robin alternando `a`, `b`, `a`, `b` em requisições consecutivas;
- link criado pela :8080 resolvendo direto na :8091 **e** na :8092;
- `kill` na instância `a` → seis 302 seguidos, todos por `b`, nenhum 502;
- instância `a` de volta → tráfego volta a dividir 3/3;
- 6 cliques pela :8080, espalhados entre as duas, viram `"cliques":6` na :8081.

### Limitações assumidas

- **Uma conexão nova por operação**, sem pool (`ponytail:` no código). Alguns ms
  de handshake por requisição; HikariCP entra se a latência incomodar.
- **Schema criado pela aplicação** (`CREATE TABLE IF NOT EXISTS`), para o
  projeto subir com um comando. Sistema de verdade usa migração versionada.
- **Um `Contador` só.** A contagem continua na memória dele; com dois, cada um
  vê só as partições que lhe couberam. O lado da escrita escala nesta fase, o da
  leitura não.
- **Tudo na mesma máquina.** É exatamente o teto que a fase 8 remove.

## Fase 8 — estado atual

`fase8/`. Mesmo código da fase 7. O que muda é tudo em volta: `Dockerfile`,
`compose.yaml` e um `nginx.conf` que fala por nome de serviço.

A fase 7 terminou com cinco processos subidos à mão, na ordem certa, exigindo
Postgres, Kafka, Nginx e Java 25 instalados na máquina. Agora:

```bash
docker compose up --build
```

### Uma imagem para dois programas

`Encurtador` e `Contador` compartilham jar e classpath; só o comando muda. Duas
imagens quase idênticas seriam duas coisas para construir, versionar e manter em
sincronia — a diferença entre elas cabe na variável `CLASSE`, e o compose a
define por serviço.

### Multi-stage: o que constrói não é o que roda

O primeiro estágio tem Maven e JDK; o segundo, só o JRE. A imagem final não
carrega Maven, JDK nem código-fonte — menos superfície e menos coisa para dar
errado do que embarcar o ambiente de build junto.

A ordem do `COPY` é deliberada: `pom.xml` sozinho antes do `src`. Enquanto as
dependências não mudam, o Docker reaproveita a camada com o `.m2` pronto, e
alterar uma linha de código não rebaixa a internet de novo.

O container roda como usuário sem privilégio: se alguém escapar do processo,
escapa para um usuário sem poder nenhum. O JRE não precisa de root para abrir a
8080.

### `depends_on: service_healthy`, não `service_started`

Container de pé não é banco pronto. O auto-teste roda no boot do `Encurtador` e
falha se o Postgres ainda estiver inicializando — a espera precisa ser pela
*saúde*, não pelo processo. Por isso Postgres e Kafka têm `healthcheck` e os
serviços dependem da condição `service_healthy`.

O `start_period: 60s` do `Encurtador` existe pelo mesmo motivo, do outro lado: o
auto-teste roda **antes** de a porta abrir, então o container demora a ficar
saudável sem estar com problema.

E aqui o `/health` da fase 7 ganha o segundo uso: além do teste, é ele que
segura o Nginx fora do ar até as duas instâncias estarem realmente atendendo.

### Nome de serviço no lugar de porta

O `nginx.conf` mudou em uma linha, e é a linha que resume a fase:

```diff
- server 127.0.0.1:8091;
- server 127.0.0.1:8092;
+ server encurtador-a:8080;
+ server encurtador-b:8080;
```

Dentro da rede do compose, `encurtador-a` resolve para o IP do container — o DNS
é do próprio Docker. Ninguém precisa saber IP de ninguém, e as duas instâncias
podem usar a **mesma** porta, porque cada uma tem sua própria pilha de rede.
Escalar deixa de significar procurar porta livre na máquina.

Só duas portas são publicadas para fora: 8080 (Nginx) e 8081 (Contador). As
instâncias do `Encurtador` não são alcançáveis de fora — quem quiser falar com
elas passa pelo load balancer, que é o desenho correto.

### Ambiente, não código

Nada precisou mudar no Java, e isso não é sorte: desde a fase 7 toda
configuração já vinha de variável de ambiente (`DB_URL`, `KAFKA_BOOTSTRAP`,
`INSTANCIA`). Trocar `localhost` por `postgres` e `kafka` foi trocar valores no
`compose.yaml`. É o pagamento da preparação feita na fase anterior.

### O volume do Postgres

`dados-postgres` existe porque sem ele `docker compose down` apagaria os links
junto com o container. Container é descartável; o banco é o único estado que
precisa sobreviver ao ciclo de vida daqui — a mesma distinção da fase 7, agora
visível na infraestrutura.

### Dois tropeços que valeram a pena

**O volume do Postgres 18 não vai em `/var/lib/postgresql/data`.** A imagem
aborta o boot se você montar direto no subdiretório: quebra o `pg_upgrade
--link` de uma futura troca de versão. O volume vai um nível acima, em
`/var/lib/postgresql`.

**O auto-teste tinha uma corrida que só o compose revelou.** A limpeza apagava
`WHERE url LIKE 'https://exemplo.com%'` — prefixo fixo. Enquanto as instâncias
subiam à mão, uma de cada vez, nunca deu problema. O compose sobe as duas
juntas, cada uma roda seu auto-teste contra a **mesma** tabela, e a limpeza de
uma apagava as linhas da outra no meio do teste. O sintoma era um clique
respondendo 404 sem nada de errado no código testado.

A correção é o mesmo padrão da fase 6 (grupo de consumidores próprio para o
teste): cada execução tem sua marca única, e só apaga o que ela criou. Vale
guardar a lição — teste que compartilha estado global funciona até o dia em que
alguém roda dois ao mesmo tempo.

### Verificação

O auto-teste roda no boot de cada container (`Auto-teste: OK (com Kafka)` nos
logs de `encurtador-a` e `encurtador-b`). Com a stack no ar:

- round-robin alternando `a`, `b` em requisições consecutivas pela :8080;
- 6 cliques dividindo 3/3 entre as instâncias, virando `"cliques":6` na :8081;
- `docker compose stop encurtador-a` → seis 302 seguidos, todos por `b`;
- `start` de volta → tráfego volta a dividir 3/3;
- `docker compose down` seguido de `up` → o link continua resolvendo, porque o
  volume sobreviveu (o teste que só esta fase permite fazer).

### Limitações assumidas

- **Senha no `compose.yaml`.** Aceitável para estudo local; um ambiente de
  verdade usa `secrets` ou um `.env` fora do versionamento.
- **Um broker, um Postgres, sem réplica.** Réplicas de banco e de broker são
  outro assunto, e grande.
- **`replication.factor: 1`** nos tópicos internos do Kafka: com um broker só,
  pedir 3 travaria a criação.
