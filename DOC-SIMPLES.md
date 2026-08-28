# Encurtador de URLs — Explicação em Português Claro

Este documento é o "par" do `DOC-TECNICO.md`. Mesmo projeto, mesma ordem,
só que sem jargão — e com um dicionário no final.

## O que estamos construindo

Sabe quando você recebe um link gigante e alguém te manda uma versão curtinha
tipo `bit.ly/abc123`? É isso. Você dá um link longo pro programa, ele te devolve
um código curto, e depois esse código te leva de volta ao link original.

Escolhi esse projeto porque ele é simples de entender **e** naturalmente puxa
todos os conceitos que você quer ver: muita gente clicando ao mesmo tempo
(virtual threads), contar os cliques sem travar o site (Kafka), e aguentar o
tranco quando o volume cresce (load balance).

## Como vamos construir: uma fase por vez

A ideia não é escrever tudo de uma vez. É subir um degrau, ver funcionando,
entender, e só então subir o próximo.

| Fase | O que fica pronto | O que você aprende |
|------|-------------------|--------------------|
| 1 ✅ | Um programinha que roda no terminal | Como um programa Java é montado |
| 2 ✅ | O mesmo link sempre vira o mesmo código, e não some ao fechar | Como evitar desperdício e como guardar em disco |
| 3 ✅ | Vira um site de verdade, abre no navegador | Como um servidor responde a um clique |
| 4 ✅ | Aguenta muita gente ao mesmo tempo | **Virtual threads** |
| 5 ✅ | Registra cada clique sem travar | **Kafka** |
| 6 ✅ | Conta e agrupa esses cliques | Como processar em segundo plano, e por que separar quem escreve de quem lê |
| 7 ✅ | Roda em várias cópias ao mesmo tempo | **Load balance** |
| 8 | Qualquer pessoa consegue rodar seu projeto | Docker (empacotamento) |

Ao final, isso é um projeto de GitHub que se defende sozinho numa entrevista.

## Fase 1 — o começo

Um arquivo só: `fase1/Encurtador.java`. Ele **não** é um site ainda. É um
programa que roda, faz o serviço, imprime na tela e acaba. Fechou, sumiu tudo —
porque ele guarda os links só na memória, e memória é temporária.

Isso é de propósito. Antes de falar de servidor, Kafka e escala, você precisa
reconhecer o formato de um programa Java quando vê um.

### As três coisas que ele sabe fazer

1. **encurtar** — você entrega um link, ele sorteia um código de 6 letras/números e guarda o par
2. **resolver** — você entrega o código, ele devolve o link original
3. **quantidade** — quantos links tem guardados

### Detalhes que valem entender

**O sorteio do código.** São 6 caracteres, tirados de um conjunto de 31
símbolos (números e consoantes, sem vogais — assim não sai palavrão por acidente).
Isso dá cerca de **887 milhões** de combinações possíveis. Se por azar sortear
um código que já existe, ele simplesmente sorteia de novo.

**Um problema que deixei de propósito.** Se você encurtar o *mesmo* link duas
vezes, ele cria dois códigos diferentes. Isso é desperdício. Não é um bug
esquecido — é o assunto da fase 2, e é mais fácil entender a solução depois de
sentir o problema.

**Ele recusa entrada vazia.** Se você mandar um link em branco, o programa dá
erro na hora, de propósito, em vez de guardar lixo. Essa checagem fica na porta
de entrada, antes de qualquer coisa ser salva. É a regra: valide na fronteira.

**Ele testa a si mesmo.** No fim do arquivo tem uma função que confere se tudo
funciona: o código tem 6 caracteres? o link volta certo? código inexistente dá
"nada"? Se qualquer resposta for não, o programa para e reclama. É o teste mais
barato que existe.

### Como rodar (depois de instalar o Java)

```bash
cd encurtador/fase1
java -ea Encurtador.java
```

O `-ea` liga o auto-teste. Sem ele o programa roda, mas não se testa.

Você deve ver algo assim (o código muda a cada execução, é sorteado):

```
Encurtado: https://www.google.com -> /k3m9xq
Resolvido: /k3m9xq -> https://www.google.com
Auto-teste: OK
```

## Fase 2 — guardando de verdade

Dois incômodos da fase 1 resolvidos.

**O mesmo link agora vira sempre o mesmo código.** Antes o código era sorteado,
então encurtar `google.com` duas vezes dava dois códigos — desperdício. Agora o
código é *calculado* a partir do próprio link, com uma conta chamada SHA-256.
Mesma entrada, mesma conta, mesma saída. Sempre. Encurtar de novo devolve o
código que já existia e não guarda nada.

Isso tem um preço, e vale saber: como o código vem do link por uma conta pública,
quem conhece a conta descobre o código de qualquer link sem perguntar ao
programa. Para um encurtador, tudo bem. Para um link secreto, não serviria.

**Os links pararam de sumir.** Agora ficam num arquivo (`links.properties`), uma
linha por link, no formato `codigo=link`. O programa lê o arquivo quando abre e
regrava quando guarda algo novo. Feche e abra de novo: continua tudo lá.

Rode duas vezes seguidas para ver as duas coisas juntas — o código não muda, e a
quantidade continua 1:

```bash
cd encurtador/fase2
java -ea Encurtador.java
```

**Um atalho que deixei de propósito.** Toda vez que guarda um link, ele reescreve
o arquivo inteiro. Com mil links ninguém sente; com milhões, sim. Está marcado no
código com um comentário `ponytail:` — troca por banco de dados quando incomodar,
não antes.

## Fase 3 — virou site

Agora é site. Antes o programa fazia o serviço e morria; agora ele **fica ligado**
esperando alguém chamar.

Suba:

```bash
cd encurtador/fase3
java -ea Encurtador.java
```

Ele avisa `Ouvindo em http://localhost:8080` e fica parado ali. Em outro
terminal, encurte um link:

```bash
curl -X POST --data 'https://www.google.com' http://localhost:8080/encurtar
```

Ele responde `/ggb1vf`. Cole `http://localhost:8080/ggb1vf` no navegador: você
vai parar no Google. Para parar o servidor, `Ctrl+C`.

**O que ele responde, e por quê.** Toda resposta HTTP vem com um número que diz
como foi:

- **201** — criei o link novo
- **301** — "mudou de endereço, vá para lá": é o redirecionamento
- **404** — esse código não existe aqui
- **400** — você mandou algo inválido (a culpa é de quem pediu)
- **405** — esse caminho existe, mas não com esse método

O **301** diz "mudou para sempre", então o navegador *decora* o destino e nas
próximas vezes vai direto, sem passar pelo nosso servidor. Ótimo para
velocidade, e vai virar um probleminha na fase 5, quando quisermos contar
cliques: clique decorado não é visto por ninguém.

**Uma trava nova de segurança.** Agora que qualquer um manda link pela rede,
o programa só aceita `http` e `https`. Sem isso alguém encurtaria
`javascript:...` e usaria o seu site para levar gente para coisa ruim.

**O que ainda falta.** Ele atende **uma pessoa por vez**. Se dois cliques
chegarem juntos, um espera o outro. É exatamente o assunto da fase 4.

## Fase 4 — aguentando muita gente

A fase 3 atendia **uma pessoa por vez**. Se dez cliques chegassem juntos, o
décimo esperava os nove da frente. Agora não.

Suba e faça a conta você mesmo:

```bash
cd encurtador/fase4
java -ea Encurtador.java
```

Em outro terminal, dispare 100 pedidos que demoram 1 segundo cada, todos juntos:

```bash
seq 100 | xargs -P100 -I{} curl -s -o /dev/null http://localhost:8080/lento
```

Terminam em pouco mais de **1 segundo**. Um por vez levaria 100. (A rota
`/lento` existe só para isso ficar visível — ela finge um trabalho demorado.)

**Por que dá para ter tantas.** Uma thread comum é cara: o sistema reserva
memória para cada uma e some no fôlego lá pelos milhares. A virtual thread é
barata, e tem uma esperteza: quando ela fica esperando algo de fora (o banco, a
rede, o disco), ela sai da frente e devolve o lugar para outra trabalhar. Como
servidor passa quase todo o tempo esperando, isso muda a escala do que dá para
atender.

**A parte que quase todo mundo erra.** Não basta ligar as virtual threads. Com
várias pessoas mexendo na mesma tabela ao mesmo tempo, a tabela comum da fase 1
**estraga em silêncio** — link some, e não aparece erro nenhum. Foram três
mudanças, e as três precisam andar juntas:

1. o servidor passou a criar uma thread leve por pedido;
2. a tabela virou uma versão feita para uso simultâneo;
3. guardar deixou de ser "olhar se existe, depois gravar" e virou um passo só.

O item 3 é sutil e vale entender: entre "olhar" e "gravar" cabe outra pessoa. Duas
threads olham juntas, as duas veem "não existe", as duas gravam, e uma apaga a
outra. O comando novo faz as duas coisas grudadas, sem esse buraco no meio.

O auto-teste prova cada uma: 200 links gravados ao mesmo tempo sem perder
nenhum; 50 pessoas encurtando o mesmo link recebendo o mesmo código; e as 100
lentas em ~1s.

**O que ainda não fizemos.** Ninguém está contando os cliques. É a fase 5.

## Fase 5 — registrando os cliques

Agora cada clique vira um **recado** depositado num correio chamado Kafka.
Ninguém conta nada ainda — só registra. Contar é a fase 6.

**A regra que manda em tudo aqui:** quem clicou não pode esperar. A pessoa quer
ir para o site de destino; anotar o clique é problema nosso. Então o programa
redireciona primeiro e só depois deposita o recado. Se o Kafka estiver fora do
ar, o clique não é anotado — e o site continua funcionando normalmente. Perder
estatística é chato; deixar o site fora do ar é grave.

**O que mudou por causa disso.** Lembra do 301 da fase 3, que fazia o navegador
decorar o destino? Ele decorava e parava de passar por nós — e clique que não
passa não é contado. Trocamos para o **302**, que quer dizer "por enquanto é
ali": o navegador volta a perguntar toda vez. Custa um pouco mais de trabalho,
em troca de enxergar o que acontece.

**Primeira vez que usamos código de fora.** Até a fase 4 tudo vinha junto com o
Java. O cliente do Kafka não vem, então entrou o **Maven** — o programa que baixa
bibliotecas de terceiros e monta o projeto. Por isso a fase 5 roda diferente:

```bash
# 1. ligar o correio (uma vez)
~/.local/opt/kafka/bin/kafka-server-start.sh -daemon ~/.local/opt/kafka/config/server.properties

# 2. subir o site
cd encurtador/fase5
mvn -q exec:exec
```

Em outro terminal, encurte e clique algumas vezes:

```bash
curl -X POST --data 'https://www.google.com/busca' http://localhost:8080/encurtar
curl -s -o /dev/null http://localhost:8080/h7bplw   # use o codigo que voltou
```

E agora espie os recados chegando:

```bash
~/.local/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cliques --from-beginning
```

Cada clique aparece como uma linha:

```json
{"codigo":"h7bplw","destino":"https://www.google.com/busca","instante":"...","agente":"curl/8.18.0"}
```

**Um detalhe esperto.** Cada recado vai com uma "chave" — no caso, o código do
link. O Kafka usa a chave para decidir em qual fila o recado entra, e recados da
mesma chave sempre caem na mesma fila, na ordem. Assim, quem for somar os cliques
de um link na fase 6 recebe tudo daquele link em ordem, sem bagunça.

**Quando você rodar, vai ver uns avisos vermelhos.** É de propósito: o auto-teste
aponta para um Kafka que não existe, só para provar que o site continua de pé
sem ele.

## Fase 6 — dois programas, um correio no meio

Na fase 5 os cliques viravam recados no correio. Agora tem alguém **retirando**
os recados e somando.

E aqui vem a ideia mais importante do projeto até agora: são **dois programas
separados**, e eles não conversam entre si.

- o **Encurtador** (porta 8080) atende quem encurta e quem clica;
- o **Contador** (porta 8081) só lê os recados e responde "esse link teve 7 cliques".

Um não chama o outro. Nunca. A única ligação entre eles é o correio. Isso tem
nome: **CQRS** — separar quem *escreve* de quem *lê*.

**Por que separar?** Porque as duas coisas têm necessidades opostas. Redirecionar
precisa ser instantâneo e acontece o tempo todo. Somar estatística pode demorar
um segundo e ninguém morre. Juntos, o relatório pesado atrapalha o clique.
Separados, cada um cresce do seu jeito — e você pode desligar o Contador para
mexer nele **sem derrubar o site**. Os recados esperam no correio, e quando ele
volta, continua exatamente de onde parou.

Suba os dois, em terminais diferentes:

```bash
cd encurtador/fase6
mvn -q exec:exec                                   # o site, porta 8080

# em outro terminal
cd encurtador/fase6
mvn -q exec:exec -Dclasse=Contador -Dporta=8081    # o contador, porta 8081
```

Encurte, clique 7 vezes, e pergunte ao contador:

```bash
curl -X POST --data 'https://exemplo.com/promo' http://localhost:8080/encurtar
# devolve /phn7x9
for i in $(seq 7); do curl -s -o /dev/null http://localhost:8080/phn7x9; done
curl http://localhost:8081/stats/phn7x9
```

```json
{"codigo":"phn7x9","cliques":7,"ultimo":"2026-08-27T00:34:38.948Z"}
```

`curl http://localhost:8081/stats` lista todos, do mais clicado para o menos.

**Uma escolha honesta que vale entender.** O contador avisa o correio "já
processei até aqui" **depois** de somar, nunca antes. Se ele cair no meio, na
volta ele refaz o último punhado de recados e algum clique conta duas vezes.
Se avisasse antes, ele *perderia* cliques. Entre contar um a mais e perder um,
para estatística é melhor contar a mais. Isso se chama **at-least-once** — "pelo
menos uma vez".

**Para ver a divisão de trabalho.** Suba um segundo Contador em outra porta: o
Kafka reparte as filas entre os dois automaticamente. Derrube um, e o outro
assume as filas dele sozinho. Isso é o **rebalanceamento**, e é o que faz um
sistema aguentar perder uma máquina sem perder trabalho.

(Detalhe: com dois contadores, cada um vê só uma parte das filas — logo, uma
parte da conta. Para somar tudo de verdade, a contagem precisaria ficar num
lugar compartilhado. Está marcado no código; é assunto de outro projeto.)

---

## Fase 7 — o que já está pronto

Até agora existia **um** programa atendendo todo mundo. Se ele caísse, o site
caía. Se muita gente chegasse junto, ele sozinho aguentava o tranco.

Agora existem **duas cópias iguais** do site, e um porteiro na frente decidindo
quem atende cada pessoa. O porteiro é o **Nginx**, e essa distribuição é o
**load balance**.

### O problema que apareceu primeiro

A ideia parece simples — é só subir o programa duas vezes. Mas aí acontece isto:

> Você encurta um link, e quem te atendeu foi a **cópia A**. Ela anota o link no
> caderninho dela. Você manda o link para um amigo, ele clica, e o porteiro
> manda o clique para a **cópia B** — que nunca ouviu falar desse link.
> Resultado: "código não encontrado".

O problema é o caderninho. Enquanto cada cópia tem o seu, elas não são cópias de
verdade, são programas diferentes fingindo ser o mesmo.

### A solução: ninguém tem caderninho

O caderninho sai de dentro das cópias e vira um **banco de dados** (o
PostgreSQL) do lado de fora, que todas consultam. Agora nenhuma cópia sabe nada
que as outras não saibam — e por isso qualquer uma pode atender qualquer pessoa.

Isso tem nome: o programa ficou **stateless** (sem estado). É a única exigência
para poder ter várias cópias, e é a ideia central desta fase.

O ganho aparece na hora: dá para matar uma cópia no meio do expediente e o site
continua no ar, porque a outra sabe exatamente as mesmas coisas. E se um dia
precisar aguentar mais gente, você sobe uma terceira, uma quarta — sem mudar
uma linha de código.

### O porteiro precisa saber quem está de pé

Cada cópia responde numa porta especial, a `/health` — o equivalente a "você
está bem?". Se ela não responde, o porteiro para de mandar gente para lá até ela
melhorar.

Um detalhe que parece besteira e não é: essa pergunta olha o **banco**, não o
correio (o Kafka). Porque sem banco a cópia realmente não consegue atender
ninguém, mas sem correio ela atende normal — só deixa de contar os cliques. Se
o "você está bem?" reprovasse por causa do correio, **todas** as cópias seriam
tiradas do ar ao mesmo tempo, e o site inteiro cairia para proteger um contador.

Isso aconteceu de verdade durante o teste: o correio caiu sozinho, e o site
continuou redirecionando sem ninguém perceber. Só os cliques daquele intervalo
se perderam.

### Como rodar

Precisa do Postgres e do Nginx instalados, e o banco criado:

```bash
sudo -u postgres psql -c "CREATE ROLE $USER LOGIN CREATEDB" \
                     -c "CREATE DATABASE encurtador OWNER $USER"
psql -d encurtador -c "ALTER ROLE $USER PASSWORD 'encurtador'"
export DB_PASSWORD=encurtador
```

Quatro terminais — duas cópias do site, o contador e o porteiro:

```bash
cd encurtador/fase7
INSTANCIA=a mvn -q exec:exec -Dporta=8091      # copia A
INSTANCIA=b mvn -q exec:exec -Dporta=8092      # copia B
mvn -q exec:exec -Dclasse=Contador -Dporta=8081

mkdir -p /tmp/nginx-encurtador/logs
nginx -p /tmp/nginx-encurtador -c $PWD/nginx.conf     # o porteiro, porta 8080
```

Agora tudo passa pela **8080** e você nem sabe quem atendeu. Para ver o rodízio,
olhe o cabeçalho `X-Instancia`:

```bash
for i in $(seq 4); do curl -s -D- -o /dev/null http://localhost:8080/health | grep -i x-instancia; done
```

```
X-instancia: a
X-instancia: b
X-instancia: a
X-instancia: b
```

E a prova de que o caderninho é compartilhado: encurte pela 8080 e resolva
direto em cada cópia — as duas conhecem o link.

```bash
curl -X POST --data 'https://exemplo.com/promo' http://localhost:8080/encurtar
# devolve /dwr89s
curl -i http://localhost:8091/dwr89s    # 302
curl -i http://localhost:8092/dwr89s    # 302 tambem
```

Para o teste favorito: derrube uma cópia (Ctrl+C) e fique clicando pela 8080. O
site não pisca — o porteiro percebe e manda tudo para a que sobrou.

### O que ainda incomoda

Tudo isso está rodando na **sua máquina**, montado à mão, num terminal por
programa. Cinco coisas para subir na ordem certa, e nenhuma garantia de que vai
funcionar igual em outro computador. É esse incômodo que a fase 8 resolve.

## Dicionário

Termos na ordem em que aparecem no projeto.

**Java** — a linguagem de programação. Você escreve um texto seguindo as regras
dela, e o computador executa.

**JDK** *(Java Development Kit)* — o "kit" que você instala para conseguir rodar
e criar programas Java. Sem ele, o arquivo `.java` é só um texto.

**LTS** *(Long Term Support)* — versão com suporte de longo prazo. Vamos usar a
Java 25 por isso: fica estável por anos, não muda debaixo de você.

**Classe** — o molde que descreve uma coisa: o que ela guarda e o que ela sabe
fazer. `Encurtador` é uma classe.

**Objeto** — uma cópia viva feita a partir do molde. A classe é a receita de
bolo; o objeto é o bolo.

**Método** — uma ação que a classe sabe executar. `encurtar` e `resolver` são
métodos. Em outras linguagens chamam de "função".

**`main`** — o método especial por onde o Java **começa**. Todo programa Java
tem um. É a porta de entrada.

**Variável** — um nome que guarda um valor. `codigo` guarda o código sorteado.

**Tipo** — que espécie de valor uma variável aceita. `String` = texto,
`int` = número inteiro. Java é rígido nisso, e isso é bom: ele te avisa do erro
antes de rodar, e não no meio do uso.

**`String`** — texto. `"https://google.com"` é uma String.

**`Map`** — uma tabela de duas colunas: chave e valor. Você guarda pela chave e
busca pela chave. É exatamente o que um encurtador precisa: código → link.

**`HashMap`** — o tipo de `Map` mais comum. "Hash" é a técnica que ele usa para
achar a chave rápido, sem varrer a lista inteira.

**Memória (RAM)** — onde o programa guarda coisas *enquanto está rodando*.
Fechou o programa, esvaziou. Por isso a fase 1 esquece tudo ao terminar.

**Persistência** — o contrário: guardar em disco ou banco de dados, para
sobreviver ao desligamento. É o que a fase 2 faz.

**Hash** — uma conta que transforma um texto de qualquer tamanho num resultado de
tamanho fixo. Mesmo texto sempre dá o mesmo resultado; texto diferente quase
sempre dá resultado diferente. **SHA-256** é uma dessas contas.

**Determinístico** — que sempre dá o mesmo resultado para a mesma entrada. O
oposto de sorteado.

**Idempotente** — fazer duas vezes tem o mesmo efeito de fazer uma. Encurtar o
mesmo link de novo não cria nada novo.

**Colisão** — quando dois textos diferentes dão o mesmo resultado. É raro, mas
possível, então o programa precisa saber o que fazer quando acontece.

**Validação** — conferir se o que chegou de fora presta, antes de usar.

**Exceção** *(exception)* — o jeito do Java dizer "deu errado". O programa
interrompe o caminho normal e sinaliza o problema, em vez de fingir que está tudo bem.

**`assert`** — uma afirmação de conferência: "isto **tem** que ser verdade".
Se não for, o programa para. É como testamos de graça, sem instalar nada.

**Compilar** — traduzir seu texto `.java` para algo que a máquina executa.
Na fase 1 o Java faz isso sozinho, escondido, quando você roda o arquivo.

**Terminal / CLI** — a tela preta de comandos. É onde você digita `java ...`.

**Git** — o programa que guarda o histórico de todas as versões do seu código.

**GitHub** — o site onde esse histórico fica hospedado e visível para os outros.
É o destino final deste projeto.

### Termos das próximas fases (para você já ir se acostumando)

**Servidor HTTP** — o programa que fica ligado esperando alguém acessar um
endereço, e responde. É o que transformou nosso programinha em site, na fase 3.

**Requisição e resposta** — o pedido que o navegador manda e o que o servidor
devolve. Toda navegação na internet é esse par, repetido.

**Código de status** — o número que vem junto da resposta dizendo como foi:
200 deu certo, 301 mudou de endereço, 404 não achei, 400 seu pedido estava
errado, 500 eu quebrei.

**Rota** — a ligação entre um endereço (`/encurtar`) e o pedaço de código que
responde por ele. "Rotear" é escolher qual código atende cada pedido.

**Porta** — o número que separa vários programas no mesmo computador. Nosso
servidor usa a 8080; o navegador precisa saber para onde bater.

**localhost** — o próprio computador. `http://localhost:8080` é "o servidor que
está rodando aqui mesmo".

**curl** — programa de terminal que faz requisições HTTP. É um navegador sem
tela, útil para testar.

**Thread** — uma "linha de execução". Um programa com uma thread faz uma coisa
por vez; com várias, atende várias pessoas ao mesmo tempo.

**Virtual thread** — a novidade do Java moderno. Thread tradicional é cara: o
sistema aguenta alguns milhares. Virtual thread é leve: dá para ter **milhões**.
Isso muda o que dá para fazer com um servidor. É o que a fase 4 usa.

**Concorrência** — várias coisas acontecendo ao mesmo tempo dentro do programa.

**Condição de corrida** *(race condition)* — quando o resultado depende de quem
chegou primeiro, e às vezes dá errado. O caso clássico: duas threads olham e as
duas gravam, e uma apaga o trabalho da outra.

**Atômico** — operação que acontece inteira ou não acontece; ninguém consegue se
enfiar no meio dela. É a cura da condição de corrida.

**Thread-safe** — código ou estrutura que aguenta várias threads ao mesmo tempo
sem estragar. `HashMap` não é; `ConcurrentHashMap` é.

**Trava** *(lock)* — combinado que deixa só uma thread por vez entrar num
trecho. Resolve, mas quem espera fica parado — use no menor pedaço possível.

**Kafka** — um "correio" entre programas. Um programa deposita mensagens, outro
retira e processa, cada um no seu ritmo. É o que a fase 5 usa para registrar
cliques sem travar o site.

**Evento** — o recado que registra que algo aconteceu ("o código h7bplw foi
clicado às 14h"). Não é um pedido, é um fato passado.

**Tópico** — a "caixa" do correio onde os recados de um mesmo assunto ficam. O
nosso se chama `cliques`.

**Partição** — cada tópico é dividido em filas, para várias pessoas poderem
retirar recados ao mesmo tempo. A ordem é garantida dentro de uma fila.

**Chave** — o dado que decide em qual fila o recado entra. Mesma chave, mesma
fila, ordem preservada.

**Produtor** — quem deposita recados. Nosso site é um.

**Consumidor** — quem retira e processa. É o Contador da fase 6.

**Grupo de consumidores** — vários consumidores com o mesmo nome de grupo, que
dividem as filas entre si. Cada fila só é lida por um membro do grupo.

**Offset** — a marca de "já li até aqui" que o consumidor deixa no correio. É
graças a ela que dá para desligar e voltar sem perder nem repetir tudo.

**Rebalanceamento** — quando entra ou sai um membro do grupo e o Kafka
redistribui as filas entre quem sobrou.

**At-least-once** *(pelo menos uma vez)* — garantia de que nada se perde, ao
preço de algo poder ser processado duas vezes. O oposto é *at-most-once*, que
nunca repete mas pode perder.

**CQRS** — separar o programa que escreve do programa que lê. Cada lado tem o
formato de dados que precisa e cresce sozinho.

**Assíncrono** — "manda e segue a vida", sem esperar a resposta. O contrário de
síncrono, onde você fica parado esperando.

**Maven** — o programa que baixa bibliotecas de terceiros e monta o projeto
Java. Entrou na fase 5, junto com a primeira dependência externa.

**Dependência** — código de outra pessoa que seu programa usa. Poupa trabalho e
cria compromisso: agora você depende da versão, dos bugs e da vida dela.

**Load balance** *(balanceamento de carga)* — quando um servidor não dá conta,
você põe vários e coloca um "porteiro" na frente distribuindo quem vai para
onde. É a fase 7.

**Stateless** *(sem estado)* — programa que não guarda nada entre um pedido e
outro; tudo que ele precisa saber está fora dele. É o que permite ter várias
cópias iguais, e por isso é a base do load balance.

**Round-robin** — a forma mais simples de distribuir: um para cada, em rodízio.
Primeiro pedido para a cópia A, segundo para a B, terceiro volta para a A.

**Health check** — o porteiro perguntando "você está bem?" para cada cópia. Quem
não responde para de receber gente até melhorar.

**Escala horizontal** — crescer somando máquinas iguais, em vez de trocar por
uma máquina maior (que seria escala *vertical*).

**Banco de dados** — programa especializado em guardar informação de forma
organizada e responder perguntas sobre ela. O nosso é o PostgreSQL, e desde a
fase 7 é onde os links moram.

**Chave primária** — a coluna que identifica cada linha do banco e não admite
repetição. É ela que impede duas cópias de gravarem o mesmo código ao mesmo
tempo — o mesmo papel que o `ConcurrentHashMap` fazia na fase 4, um andar acima.

**Docker** — empacota seu programa com tudo que ele precisa, para rodar igual em
qualquer máquina. Acaba com o "na minha máquina funciona". É a fase 8.
