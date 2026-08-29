import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Fase 7 do encurtador de URLs.
 *
 * Novidade: o estado saiu de dentro do processo. Ate a fase 6 o mapa
 * codigo->URL vivia num HashMap e num arquivo local; agora vive no Postgres, e
 * o processo nao guarda nada entre uma requisicao e outra. Isso e ser STATELESS,
 * e e o que permite subir varias copias identicas atras de um load balancer:
 * qualquer instancia responde qualquer requisicao, porque nenhuma sabe nada que
 * as outras nao saibam.
 *
 * Com o arquivo local isso nao funcionava: encurtar na instancia A e clicar na
 * instancia B dava 404, porque o link so existia na memoria (e no disco) de A.
 *
 * Continua valendo tudo da fase 5: o clique nao espera o Kafka. Mas note a
 * assimetria - o banco e dependencia DURA (sem ele nao ha o que redirecionar) e
 * o Kafka e dependencia MOLE (sem ele so perdemos estatistica). /health reflete
 * exatamente isso: pergunta pelo banco, nao pelo broker.
 */
public class Encurtador {

    /**
     * Como falar com o banco.
     *
     * ponytail: uma conexao nova por operacao, sem pool. Simples e correto,
     * mas o handshake do Postgres custa alguns ms por requisicao. Se a latencia
     * do redirect incomodar, entra HikariCP - e so trocar o que ha aqui dentro.
     */
    private final String jdbcUrl;
    private final String usuario;
    private final String senha;

    /** Para onde os eventos de clique vao. Pode ser null: sem Kafka, o site funciona igual. */
    private final Producer<String, String> produtor;

    static final String TOPICO = "cliques";

    /**
     * Quem respondeu. So existe para o load balancer ficar visivel: com varias
     * copias identicas atras do Nginx, sem isso nao da para ver o round-robin
     * acontecendo. Vai no header X-Instancia de toda resposta.
     */
    static final String INSTANCIA = System.getenv().getOrDefault("INSTANCIA", "sem-nome");

    private static final String ALFABETO = "bcdfghjklmnpqrstvwxyz0123456789";
    private static final int TAMANHO_CODIGO = 6;

    public Encurtador(String jdbcUrl, String usuario, String senha, Producer<String, String> produtor) {
        this.jdbcUrl = jdbcUrl;
        this.usuario = usuario;
        this.senha = senha;
        this.produtor = produtor;
        criarTabela();
    }

    private Connection conectar() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, usuario, senha);
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

    /**
     * O mesmo putIfAbsent da fase 4, agora dentro do banco.
     *
     * ON CONFLICT DO NOTHING e a versao SQL do check-then-act resolvido de forma
     * atomica: duas instancias encurtando a mesma URL no mesmo instante nao
     * conseguem gravar linhas diferentes, porque quem decide e a chave primaria.
     * Na fase 4 o ConcurrentHashMap protegia uma JVM; agora o banco protege
     * TODAS as instancias, que e o que a fase 7 precisa.
     *
     * Sem o RETURNING dava para gravar e nao saber se a linha era nossa: quando
     * o INSERT nao grava nada, o ResultSet vem vazio, e ai vamos ler quem ja
     * estava la para descobrir se e a mesma URL (mesmo codigo) ou uma colisao de
     * hash (proxima tentativa).
     */
    public String encurtar(String urlOriginal) {
        validar(urlOriginal);
        for (int tentativa = 0; ; tentativa++) {
            String codigo = gerarCodigo(urlOriginal, tentativa);
            try (Connection c = conectar()) {
                try (PreparedStatement ins = c.prepareStatement(
                        "INSERT INTO links (codigo, url) VALUES (?, ?)"
                                + " ON CONFLICT (codigo) DO NOTHING RETURNING codigo")) {
                    ins.setString(1, codigo);
                    ins.setString(2, urlOriginal);
                    try (ResultSet rs = ins.executeQuery()) {
                        if (rs.next()) {
                            return codigo; // a linha e nossa
                        }
                    }
                }
                // Ja existia: mesma URL devolve o mesmo codigo, outra URL e colisao.
                String jaGravado = buscar(c, codigo);
                if (urlOriginal.equals(jaGravado)) {
                    return codigo;
                }
            } catch (SQLException e) {
                throw new IllegalStateException("banco indisponivel ao encurtar", e);
            }
        }
    }

    public String resolver(String codigo) {
        try (Connection c = conectar()) {
            return buscar(c, codigo);
        } catch (SQLException e) {
            throw new IllegalStateException("banco indisponivel ao resolver", e);
        }
    }

    private static String buscar(Connection c, String codigo) throws SQLException {
        try (PreparedStatement sel = c.prepareStatement("SELECT url FROM links WHERE codigo = ?")) {
            sel.setString(1, codigo);
            try (ResultSet rs = sel.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    public int quantidade() {
        try (Connection c = conectar();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM links")) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            throw new IllegalStateException("banco indisponivel ao contar", e);
        }
    }

    /** O banco esta de pe? E a unica pergunta que /health precisa fazer. */
    public boolean saudavel() {
        try (Connection c = conectar();
             Statement st = c.createStatement()) {
            st.executeQuery("SELECT 1").close();
            return true;
        } catch (SQLException fora) {
            return false;
        }
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

    /**
     * Sobe a tabela se ela ainda nao existir, do mesmo jeito que o Contador sobe
     * o topico. Varias instancias fazem isso ao mesmo tempo no boot: IF NOT
     * EXISTS deixa a corrida sem efeito.
     *
     * ponytail: schema criado pela aplicacao. Serve para o projeto subir com um
     * comando so; um sistema de verdade usa migracao versionada (Flyway) para
     * conseguir evoluir a tabela sem apagar dado.
     */
    private void criarTabela() {
        try (Connection c = conectar(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS links ("
                    + "codigo TEXT PRIMARY KEY,"
                    + "url TEXT NOT NULL,"
                    + "criado_em TIMESTAMPTZ NOT NULL DEFAULT now())");
        } catch (SQLException e) {
            throw new IllegalStateException("nao consegui preparar o banco em " + jdbcUrl, e);
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
        } else if (metodo.equals("GET") && caminho.equals("/health")) {
            health(troca);
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

    /**
     * Health check: o load balancer pergunta, e a resposta decide se esta
     * instancia continua recebendo trafego.
     *
     * Pergunta pelo BANCO e nao pelo Kafka, e a diferenca e o proposito do
     * endpoint: sem banco esta instancia nao consegue redirecionar ninguem, e
     * tirar ela da rotacao ajuda; sem Kafka ela atende normalmente, e tirar ela
     * da rotacao so derrubaria o site inteiro junto com a estatistica.
     *
     * Nao toca em disco nem faz trabalho pesado - e chamado de segundo em
     * segundo, para sempre.
     */
    private void health(HttpExchange troca) throws IOException {
        boolean ok = saudavel();
        responder(troca, ok ? 200 : 503, (ok ? "ok" : "sem banco") + " " + INSTANCIA + "\n");
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
        troca.getResponseHeaders().set("X-Instancia", INSTANCIA);
        troca.sendResponseHeaders(302, -1);
        troca.close();

        // Depois de responder: o usuario ja foi embora, agora contamos com calma.
        publicarClique(codigo, destino, troca.getRequestHeaders().getFirst("User-Agent"));
    }

    private static void responder(HttpExchange troca, int status, String corpo) throws IOException {
        byte[] bytes = corpo.getBytes(StandardCharsets.UTF_8);
        troca.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        troca.getResponseHeaders().set("X-Instancia", INSTANCIA);
        troca.sendResponseHeaders(status, bytes.length);
        try (var out = troca.getResponseBody()) {
            out.write(bytes);
        }
    }

    // -------------------------------------------------------------------- main

    /** Onde fica o banco. Tudo por ambiente: as instancias so diferem no ambiente. */
    static final String DB_URL = System.getenv()
            .getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/encurtador");
    static final String DB_USER = System.getenv().getOrDefault("DB_USER", System.getProperty("user.name"));
    static final String DB_PASSWORD = System.getenv().getOrDefault("DB_PASSWORD", "");

    public static void main(String[] args) throws IOException {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        AutoTeste.rodar(bootstrap);

        int porta = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        Producer<String, String> produtor = produtorPadrao(bootstrap);
        Encurtador app = new Encurtador(DB_URL, DB_USER, DB_PASSWORD, produtor);
        HttpServer servidor = app.servir(porta);

        // Fechar o produtor esvazia o que ainda estava na fila de envio.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            servidor.stop(0);
            produtor.close();
        }));

        System.out.println("Instancia '" + INSTANCIA + "' ouvindo em http://localhost:" + porta);
        System.out.println("  banco: " + DB_URL + " (" + app.quantidade() + " links)");
        System.out.println("  kafka: " + bootstrap + " | topico '" + TOPICO + "'");
        System.out.println("  curl -X POST --data 'https://www.google.com' http://localhost:" + porta + "/encurtar");
        System.out.println("  curl -i http://localhost:" + porta + "/<codigo>");
        System.out.println("  curl http://localhost:" + porta + "/health");
        System.out.println("Ctrl+C para parar.");
    }
}
