import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.time.Duration;

/**
 * Fase 6: o lado da LEITURA.
 *
 * Programa separado do Encurtador. Os dois nunca se falam direto - so pelo
 * Kafka. Isso tem nome: CQRS. Quem escreve (encurtar, redirecionar) e quem le
 * (estatisticas) sao sistemas diferentes, com modelos de dados diferentes,
 * escalando de forma independente.
 *
 * Consequencia pratica: derrubar o Contador nao afeta ninguem clicando; os
 * eventos ficam no Kafka esperando, e quando ele volta continua de onde parou.
 */
public class Contador {

    /** codigo -> quantos cliques. LongAdder porque varios somam ao mesmo tempo. */
    private final Map<String, LongAdder> cliques = new ConcurrentHashMap<>();

    /** codigo -> quando foi o ultimo clique. */
    private final Map<String, Instant> ultimoClique = new ConcurrentHashMap<>();

    private final AtomicBoolean rodando = new AtomicBoolean(true);

    static final String GRUPO = "contador-cliques";

    // ------------------------------------------------------------------ leitura

    public long total(String codigo) {
        LongAdder a = cliques.get(codigo);
        return a == null ? 0 : a.sum();
    }

    public int codigosConhecidos() {
        return cliques.size();
    }

    // ------------------------------------------------------------------ consumo

    /**
     * Fica retirando eventos do topico e somando. Roda ate parar() ser chamado.
     *
     * enable.auto.commit=false: nos avisamos o Kafka onde paramos, e so DEPOIS
     * de processar o lote. Isso e at-least-once - se cairmos entre processar e
     * avisar, na volta reprocessamos o lote e algum clique conta duas vezes.
     * O contrario (avisar antes) perderia cliques, que e pior para estatistica.
     */
    public void consumir(String bootstrap) {
        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(config(bootstrap))) {
            consumidor.subscribe(List.of(Encurtador.TOPICO));
            while (rodando.get()) {
                ConsumerRecords<String, String> lote = consumidor.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> evento : lote) {
                    // A chave e o codigo, e o timestamp vem do proprio registro:
                    // nao precisamos abrir o JSON para contar.
                    cliques.computeIfAbsent(evento.key(), c -> new LongAdder()).increment();
                    ultimoClique.put(evento.key(), Instant.ofEpochMilli(evento.timestamp()));
                }
                if (!lote.isEmpty()) {
                    consumidor.commitSync(); // "processei ate aqui"
                }
            }
        }
    }

    public void parar() {
        rodando.set(false);
    }

    private static Properties config(String bootstrap) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        // Mesmo grupo = os membros DIVIDEM as particoes entre si. Suba dois
        // Contadores e o Kafka reparte; derrube um e o outro assume (rebalance).
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GRUPO);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return p;
    }

    /**
     * Garante o topico com 3 particoes. Uma particao so aguenta um consumidor
     * ativo por grupo - com 3 da para ver a divisao e o rebalanceamento.
     */
    static void prepararTopico(String bootstrap, int particoes) {
        // default.api.timeout.ms nunca pode ser menor que request.timeout.ms.
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap,
                "request.timeout.ms", "5000", "default.api.timeout.ms", "10000"))) {
            try {
                admin.createTopics(List.of(new NewTopic(Encurtador.TOPICO, particoes, (short) 1))).all().get();
            } catch (Exception jaExiste) {
                try {
                    // Da para AUMENTAR particoes, nunca diminuir.
                    admin.createPartitions(Map.of(Encurtador.TOPICO, NewPartitions.increaseTo(particoes)))
                            .all().get();
                } catch (Exception jaTemOSuficiente) {
                    // ok: o topico ja esta do tamanho que precisamos
                }
            }
        }
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
        String caminho = troca.getRequestURI().getPath();
        if (!troca.getRequestMethod().equals("GET")) {
            responder(troca, 405, "so GET aqui\n");
        } else if (caminho.equals("/stats")) {
            responder(troca, 200, todasAsEstatisticas());
        } else if (caminho.startsWith("/stats/")) {
            responder(troca, 200, estatistica(caminho.substring("/stats/".length())));
        } else {
            responder(troca, 404, "GET /stats ou GET /stats/<codigo>\n");
        }
    }

    /** Codigo sem clique nenhum devolve zero, nao 404: "nunca clicaram" e uma resposta. */
    private String estatistica(String codigo) {
        return """
                {"codigo":"%s","cliques":%d,"ultimo":%s}
                """.formatted(codigo, total(codigo), aspas(ultimoClique.get(codigo)));
    }

    private String todasAsEstatisticas() {
        StringJoiner json = new StringJoiner(",\n  ", "[\n  ", "\n]\n");
        cliques.entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<String, LongAdder> e) -> e.getValue().sum()).reversed())
                .forEach(e -> json.add("""
                        {"codigo":"%s","cliques":%d,"ultimo":%s}"""
                        .formatted(e.getKey(), e.getValue().sum(), aspas(ultimoClique.get(e.getKey())))));
        return cliques.isEmpty() ? "[]\n" : json.toString();
    }

    private static String aspas(Instant i) {
        return i == null ? "null" : "\"" + i + "\"";
    }

    private static void responder(HttpExchange troca, int status, String corpo) throws IOException {
        byte[] bytes = corpo.getBytes(StandardCharsets.UTF_8);
        troca.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        troca.sendResponseHeaders(status, bytes.length);
        try (var out = troca.getResponseBody()) {
            out.write(bytes);
        }
    }

    // -------------------------------------------------------------------- main

    public static void main(String[] args) throws IOException {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        int porta = args.length > 0 ? Integer.parseInt(args[0]) : 8081;

        prepararTopico(bootstrap, 3);

        Contador contador = new Contador();
        HttpServer servidor = contador.servir(porta);
        Thread consumo = Thread.ofVirtual().start(() -> contador.consumir(bootstrap));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            contador.parar();
            servidor.stop(1);
        }));

        System.out.println("Contador ouvindo em http://localhost:" + porta
                + " | grupo '" + GRUPO + "' no topico '" + Encurtador.TOPICO + "'");
        System.out.println("  curl http://localhost:" + porta + "/stats");
        System.out.println("  curl http://localhost:" + porta + "/stats/<codigo>");
        System.out.println("Ctrl+C para parar.");

        try {
            consumo.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * ponytail: a contagem vive na memoria desta instancia. Sobe dois
     * Contadores no mesmo grupo e cada um fica com uma parte das particoes -
     * logo, com uma parte da contagem. Para somar de verdade em varias
     * instancias, a contagem precisa ir para um lugar compartilhado (banco,
     * Redis) ou virar Kafka Streams com store. Fora do escopo do estudo.
     */
}
