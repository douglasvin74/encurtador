import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * Fase 5 do encurtador de URLs.
 *
 * Novidade: cada clique vira um EVENTO publicado no Kafka.
 *
 * A regra que manda em tudo aqui: o clique nao pode esperar o Kafka. Quem
 * clicou quer ser redirecionado; contar o clique e problema nosso, nao dele.
 * Por isso a publicacao e assincrona e o erro dela nunca vira erro do usuario -
 * Kafka fora do ar significa estatistica perdida, nao site fora do ar.
 *
 * Quem consome esses eventos e conta e a fase 6.
 */
public class Encurtador {

    private final Map<String, String> links = new ConcurrentHashMap<>();
    private final Path arquivo;

    /** Para onde os eventos de clique vao. Pode ser null: sem Kafka, o site funciona igual. */
    private final Producer<String, String> produtor;

    static final String TOPICO = "cliques";

    private static final String ALFABETO = "bcdfghjklmnpqrstvwxyz0123456789";
    private static final int TAMANHO_CODIGO = 6;

    public Encurtador(Path arquivo, Producer<String, String> produtor) {
        this.arquivo = arquivo;
        this.produtor = produtor;
        carregar();
    }

    /**
     * Monta o produtor padrao.
     *
     * max.block.ms curto de proposito: se o broker sumir, send() desiste rapido
     * em vez de segurar a thread do redirect. Preferimos perder a estatistica.
     */
    public static Producer<String, String> produtorPadrao(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 20); // junta eventos por 20ms antes de mandar
        p.put(ProducerConfig.ACKS_CONFIG, "1");     // 1 replica basta: e estatistica, nao dinheiro
        return new KafkaProducer<>(p);
    }

    // ---------------------------------------------------------------- dominio

    public String encurtar(String urlOriginal) {
        validar(urlOriginal);
        for (int tentativa = 0; ; tentativa++) {
            String codigo = gerarCodigo(urlOriginal, tentativa);
            String jaGravado = links.putIfAbsent(codigo, urlOriginal);
            if (jaGravado == null) {
                salvar();
                return codigo;
            }
            if (jaGravado.equals(urlOriginal)) {
                return codigo;
            }
        }
    }

    public String resolver(String codigo) {
        return links.get(codigo);
    }

    public int quantidade() {
        return links.size();
    }

    private static void validar(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("URL vazia");
        }
        String esquema;
        try {
            esquema = new URI(url).getScheme();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("URL malformada");
        }
        if (!"http".equalsIgnoreCase(esquema) && !"https".equalsIgnoreCase(esquema)) {
            throw new IllegalArgumentException("so aceito http ou https");
        }
    }

    private static String gerarCodigo(String url, int tentativa) {
        byte[] hash = sha256(url + "#" + tentativa);
        StringBuilder sb = new StringBuilder(TAMANHO_CODIGO);
        for (int i = 0; i < TAMANHO_CODIGO; i++) {
            sb.append(ALFABETO.charAt((hash[i] & 0xff) % ALFABETO.length()));
        }
        return sb.toString();
    }

    private static byte[] sha256(String texto) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(texto.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossivel) {
            throw new IllegalStateException(impossivel);
        }
    }

    // ------------------------------------------------------------ persistencia

    private void carregar() {
        if (!Files.exists(arquivo)) {
            return;
        }
        Properties p = new Properties();
        try (var in = Files.newInputStream(arquivo)) {
            p.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("nao consegui ler " + arquivo, e);
        }
        p.forEach((codigo, url) -> links.put((String) codigo, (String) url));
    }

    /**
     * ponytail: trava global + reescrita do arquivo inteiro por gravacao.
     * Gargalo de escrita assumido; leitura (o redirect) nao passa por aqui.
     */
    private synchronized void salvar() {
        Properties p = new Properties();
        p.putAll(links);
        try (var out = Files.newOutputStream(arquivo)) {
            p.store(out, "encurtador - codigo=url");
        } catch (IOException e) {
            throw new UncheckedIOException("nao consegui gravar " + arquivo, e);
        }
    }

    // -------------------------------------------------------------------- kafka

    /**
     * Publica o clique e volta na hora - nao espera confirmacao do broker.
     *
     * A chave e o codigo: o Kafka usa a chave para escolher a particao, entao
     * todos os cliques do mesmo codigo caem na mesma particao e chegam na
     * ordem. Isso importa para quem for somar do outro lado (fase 6).
     */
    private void publicarClique(String codigo, String destino, String agente) {
        if (produtor == null) {
            return; // rodando sem Kafka: segue o jogo
        }
        String evento = """
                {"codigo":"%s","destino":"%s","instante":"%s","agente":"%s"}"""
                .formatted(escapar(codigo), escapar(destino), Instant.now(), escapar(agente));
        try {
            produtor.send(new ProducerRecord<>(TOPICO, codigo, evento), (metadados, erro) -> {
                if (erro != null) {
                    // Perdemos a estatistica deste clique. O usuario ja foi redirecionado.
                    System.err.println("clique nao publicado: " + erro.getMessage());
                }
            });
        } catch (Exception falhaAoEnfileirar) {
            System.err.println("clique nao enfileirado: " + falhaAoEnfileirar.getMessage());
        }
    }

    /** JSON na mao: so precisa escapar aspas e barra invertida para nao quebrar o formato. */
    private static String escapar(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ----------------------------------------------------------------- servidor

    public HttpServer servir(int porta) throws IOException {
        HttpServer servidor = HttpServer.create(new InetSocketAddress(porta), 0);
        servidor.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        servidor.createContext("/", this::rotear);
        servidor.start();
        return servidor;
    }

    private void rotear(HttpExchange troca) throws IOException {
        String metodo = troca.getRequestMethod();
        String caminho = troca.getRequestURI().getPath();

        if (metodo.equals("POST") && caminho.equals("/encurtar")) {
            postEncurtar(troca);
        } else if (metodo.equals("GET") && caminho.equals("/")) {
            responder(troca, 200, "POST /encurtar com a URL no corpo. GET /<codigo> redireciona.\n");
        } else if (metodo.equals("GET") && caminho.equals("/lento")) {
            lento(troca);
        } else if (metodo.equals("GET")) {
            getRedirecionar(troca, caminho.substring(1));
        } else {
            responder(troca, 405, "metodo nao suportado aqui\n");
        }
    }

    private void lento(HttpExchange troca) throws IOException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        responder(troca, 200, "demorei 1s em " + Thread.currentThread() + "\n");
    }

    private void postEncurtar(HttpExchange troca) throws IOException {
        String url = new String(troca.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        try {
            responder(troca, 201, "/" + encurtar(url) + "\n");
        } catch (IllegalArgumentException recusada) {
            responder(troca, 400, recusada.getMessage() + "\n");
        }
    }

    /**
     * Redireciona e publica o clique.
     *
     * Mudou de 301 para 302 nesta fase, e o motivo e o Kafka: 301 e "mudou para
     * sempre", entao o navegador decora o destino e nas proximas vezes NAO passa
     * mais por aqui - clique invisivel, estatistica furada. 302 e "por enquanto
     * e ali", entao ele volta a perguntar toda vez e todo clique e contado.
     *
     * Troca consciente: um pouco mais de trabalho para o servidor, em troca de
     * saber o que esta acontecendo. Quem nao precisa contar deve ficar no 301.
     */
    private void getRedirecionar(HttpExchange troca, String codigo) throws IOException {
        String destino = resolver(codigo);
        if (destino == null) {
            responder(troca, 404, "codigo nao encontrado\n");
            return;
        }
        troca.getResponseHeaders().set("Location", destino);
        troca.sendResponseHeaders(302, -1);
        troca.close();

        // Depois de responder: o usuario ja foi embora, agora contamos com calma.
        publicarClique(codigo, destino, troca.getRequestHeaders().getFirst("User-Agent"));
    }

    private static void responder(HttpExchange troca, int status, String corpo) throws IOException {
        byte[] bytes = corpo.getBytes(StandardCharsets.UTF_8);
        troca.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        troca.sendResponseHeaders(status, bytes.length);
        try (var out = troca.getResponseBody()) {
            out.write(bytes);
        }
    }

    // -------------------------------------------------------------------- main

    public static void main(String[] args) throws IOException {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        AutoTeste.rodar(bootstrap);

        int porta = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        Producer<String, String> produtor = produtorPadrao(bootstrap);
        // Uma copia por instancia: a fase 7 sobe varias, e todas no mesmo arquivo se atropelam.
        String arquivo = System.getenv().getOrDefault("LINKS_FILE", "links.properties");
        Encurtador app = new Encurtador(Path.of(arquivo), produtor);
        HttpServer servidor = app.servir(porta);

        // Fechar o produtor esvazia o que ainda estava na fila de envio.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            servidor.stop(0);
            produtor.close();
        }));

        System.out.println("Ouvindo em http://localhost:" + porta + " | Kafka em " + bootstrap
                + " | topico '" + TOPICO + "'");
        System.out.println("  curl -X POST --data 'https://www.google.com' http://localhost:" + porta + "/encurtar");
        System.out.println("  curl -i http://localhost:" + porta + "/<codigo>");
        System.out.println("  ver os eventos chegando:");
        System.out.println("    ~/.local/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "
                + bootstrap + " --topic " + TOPICO + " --from-beginning");
        System.out.println("Ctrl+C para parar.");
    }
}
