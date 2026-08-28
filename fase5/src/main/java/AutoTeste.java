import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Auto-teste da fase 5. Ficou em arquivo separado porque agora ele tambem
 * precisa CONSUMIR do Kafka para conferir que o evento chegou de verdade.
 *
 * Sem framework de teste ainda: assert continua bastando, e o que importa aqui
 * e integracao (servidor de verdade + broker de verdade), nao teste de unidade.
 */
final class AutoTeste {

    static void rodar(String bootstrap) throws IOException {
        contratoHttp();
        degradaSemKafka();

        if (brokerNoAr(bootstrap)) {
            eventoChegaNoKafka(bootstrap);
            System.out.println("Auto-teste: OK (com Kafka)");
        } else {
            System.out.println("Auto-teste: OK (sem Kafka - broker fora do ar em " + bootstrap + ")");
            System.out.println("  Suba o broker para testar a publicacao de eventos:");
            System.out.println("    ~/.local/opt/kafka/bin/kafka-server-start.sh -daemon"
                    + " ~/.local/opt/kafka/config/server.properties");
        }
    }

    /** O contrato das fases 3 e 4 continua valendo - so o 301 virou 302. */
    private static void contratoHttp() throws IOException {
        comServidor(null, (app, base, cliente) -> {
            var criado = post(cliente, base + "/encurtar", "https://exemplo.com");
            assert criado.statusCode() == 201 : "criar deveria dar 201";
            String codigo = criado.body().trim();

            var redirect = get(cliente, base + codigo);
            assert redirect.statusCode() == 302 : "fase 5 redireciona com 302, deu " + redirect.statusCode();
            assert redirect.headers().firstValue("Location").orElse("").equals("https://exemplo.com")
                    : "Location errado";

            assert get(cliente, base + "/naoexiste").statusCode() == 404 : "inexistente deveria dar 404";
            assert post(cliente, base + "/encurtar", "").statusCode() == 400 : "URL vazia deveria dar 400";
            assert post(cliente, base + "/encurtar", "javascript:alert(1)").statusCode() == 400
                    : "esquema perigoso deveria dar 400";
            assert codigo.equals(post(cliente, base + "/encurtar", "https://exemplo.com").body().trim())
                    : "mesma URL deveria dar o mesmo codigo";
        });
    }

    /**
     * O teste mais importante da fase: Kafka fora do ar NAO pode derrubar o
     * redirect. Apontamos o produtor para uma porta onde nao ha ninguem.
     */
    private static void degradaSemKafka() throws IOException {
        System.out.println("  (os WARN de conexao a seguir sao o teste: broker morto de proposito)");
        Producer<String, String> produtorQuebrado = Encurtador.produtorPadrao("localhost:9099");
        try {
            comServidor(produtorQuebrado, (app, base, cliente) -> {
                String codigo = post(cliente, base + "/encurtar", "https://exemplo.com").body().trim();
                long inicio = System.nanoTime();
                var redirect = get(cliente, base + codigo);
                long ms = (System.nanoTime() - inicio) / 1_000_000;

                assert redirect.statusCode() == 302 : "sem Kafka o redirect deveria continuar funcionando";
                assert ms < 4000 : "o redirect esperou " + ms + "ms pelo Kafka - deveria nao esperar";
            });
        } finally {
            produtorQuebrado.close(Duration.ZERO);
        }
    }

    /** Fim a fim: clica, e o evento aparece no topico com a chave certa. */
    private static void eventoChegaNoKafka(String bootstrap) throws IOException {
        Producer<String, String> produtor = Encurtador.produtorPadrao(bootstrap);
        try {
            comServidor(produtor, (app, base, cliente) -> {
                String url = "https://exemplo.com/clique/" + UUID.randomUUID();
                String codigo = post(cliente, base + "/encurtar", url).body().trim().substring(1);

                assert get(cliente, base + "/" + codigo).statusCode() == 302 : "clique deveria redirecionar";

                // Grupo novo lendo desde o inicio: entrar no grupo leva alguns
                // segundos, e com "latest" o evento ja teria passado quando o
                // consumidor chegasse. Procuramos pela chave, que e unica.
                try (KafkaConsumer<String, String> consumidor = consumidor(bootstrap)) {
                    consumidor.subscribe(List.of(Encurtador.TOPICO));
                    produtor.flush(); // no site nao esperamos; no teste sim, para poder conferir

                    String evento = esperarEvento(consumidor, codigo);
                    assert evento != null : "o clique nao apareceu no topico " + Encurtador.TOPICO;
                    assert evento.contains(url) : "o evento deveria trazer o destino: " + evento;
                    assert evento.contains("\"instante\"") : "o evento deveria trazer o instante: " + evento;
                }
            });
        } finally {
            produtor.close();
        }
    }

    private static String esperarEvento(KafkaConsumer<String, String> consumidor, String chave) {
        long limite = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < limite) {
            for (ConsumerRecord<String, String> r : consumidor.poll(Duration.ofMillis(500))) {
                if (chave.equals(r.key())) {
                    return r.value();
                }
            }
        }
        return null;
    }

    private static KafkaConsumer<String, String> consumidor(String bootstrap) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "auto-teste-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(p);
    }

    private static boolean brokerNoAr(String bootstrap) {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap,
                "default.api.timeout.ms", "3000", "request.timeout.ms", "2000"))) {
            admin.listTopics().names().get();
            return true;
        } catch (Exception foraDoAr) {
            return false;
        }
    }

    // -------------------------------------------------------------- utilitarios

    /** Sobe servidor em porta livre, roda o teste, e derruba tudo no fim. */
    private static void comServidor(Producer<String, String> produtor, Teste teste) throws IOException {
        Path temp = Files.createTempFile("encurtador-teste", ".properties");
        Files.delete(temp);
        Encurtador app = new Encurtador(temp, produtor);
        HttpServer servidor = app.servir(0);
        try (HttpClient cliente = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .build()) {
            teste.executar(app, "http://localhost:" + servidor.getAddress().getPort(), cliente);
        } catch (InterruptedException interrompido) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrompido);
        } finally {
            // stop(1): da 1s para o handler em curso terminar. Com stop(0) o
            // produtor fecharia antes da publicacao assincrona do ultimo clique.
            servidor.stop(1);
            Files.deleteIfExists(temp);
        }
    }

    @FunctionalInterface
    private interface Teste {
        void executar(Encurtador app, String base, HttpClient cliente) throws IOException, InterruptedException;
    }

    private static HttpResponse<String> post(HttpClient c, String url, String corpo)
            throws IOException, InterruptedException {
        return c.send(HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(corpo)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(HttpClient c, String url)
            throws IOException, InterruptedException {
        return c.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private AutoTeste() {
    }
}
