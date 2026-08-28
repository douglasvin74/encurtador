import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * Auto-teste da fase 6.
 *
 * O que muda em relacao a fase 5: agora o teste fecha o circuito. Ele clica no
 * lado da escrita e confere que o numero apareceu no lado da leitura, com o
 * Kafka no meio. E o unico jeito honesto de testar CQRS - as duas pontas nao se
 * conhecem, so o comportamento fim a fim prova que a ligacao existe.
 */
final class AutoTeste {

    static void rodar(String bootstrap) throws IOException {
        contratoHttp();
        degradaSemKafka();

        if (brokerNoAr(bootstrap)) {
            circuitoCompleto(bootstrap);
            System.out.println("Auto-teste: OK (com Kafka)");
        } else {
            System.out.println("Auto-teste: OK (sem Kafka - broker fora do ar em " + bootstrap + ")");
            System.out.println("  Suba o broker para testar a contagem:");
            System.out.println("    ~/.local/opt/kafka/bin/kafka-server-start.sh -daemon"
                    + " ~/.local/opt/kafka/config/server.properties");
        }
    }

    /** O contrato do lado da escrita nao mudou desde a fase 5. */
    private static void contratoHttp() throws IOException {
        comEncurtador(null, (app, base, cliente) -> {
            var criado = post(cliente, base + "/encurtar", "https://exemplo.com");
            assert criado.statusCode() == 201 : "criar deveria dar 201";
            String codigo = criado.body().trim();

            var redirect = get(cliente, base + codigo);
            assert redirect.statusCode() == 302 : "deveria redirecionar com 302";
            assert redirect.headers().firstValue("Location").orElse("").equals("https://exemplo.com")
                    : "Location errado";

            assert get(cliente, base + "/naoexiste").statusCode() == 404 : "inexistente deveria dar 404";
            assert post(cliente, base + "/encurtar", "").statusCode() == 400 : "URL vazia deveria dar 400";
            assert post(cliente, base + "/encurtar", "javascript:alert(1)").statusCode() == 400
                    : "esquema perigoso deveria dar 400";
        });
    }

    /** Kafka fora do ar nao pode derrubar o redirect. */
    private static void degradaSemKafka() throws IOException {
        System.out.println("  (os WARN de conexao a seguir sao o teste: broker morto de proposito)");
        Producer<String, String> produtorQuebrado = Encurtador.produtorPadrao("localhost:9099");
        try {
            comEncurtador(produtorQuebrado, (app, base, cliente) -> {
                String codigo = post(cliente, base + "/encurtar", "https://exemplo.com").body().trim();
                long inicio = System.nanoTime();
                assert get(cliente, base + codigo).statusCode() == 302
                        : "sem Kafka o redirect deveria continuar funcionando";
                long ms = (System.nanoTime() - inicio) / 1_000_000;
                assert ms < 4000 : "o redirect esperou " + ms + "ms pelo Kafka - deveria nao esperar";
            });
        } finally {
            produtorQuebrado.close(Duration.ZERO);
        }
    }

    /** Escrita -> Kafka -> leitura: 5 cliques de um lado viram 5 no outro. */
    private static void circuitoCompleto(String bootstrap) throws IOException {
        Contador.prepararTopico(bootstrap, 3);

        Contador contador = new Contador();
        HttpServer statsHttp = contador.servir(0);
        Thread consumo = Thread.ofVirtual().start(() -> contador.consumir(bootstrap));

        Producer<String, String> produtor = Encurtador.produtorPadrao(bootstrap);
        try {
            String stats = "http://localhost:" + statsHttp.getAddress().getPort();
            comEncurtador(produtor, (app, base, cliente) -> {
                String url = "https://exemplo.com/contagem/" + UUID.randomUUID();
                String codigo = post(cliente, base + "/encurtar", url).body().trim().substring(1);

                assert contador.total(codigo) == 0 : "codigo novo deveria comecar zerado";

                for (int i = 0; i < 5; i++) {
                    assert get(cliente, base + "/" + codigo).statusCode() == 302 : "clique deveria redirecionar";
                }
                produtor.flush(); // no site nao esperamos; aqui sim, para poder conferir

                long contagem = esperarContagem(contador, codigo, 5);
                assert contagem == 5 : "esperava 5 cliques contados, vieram " + contagem;

                // A mesma contagem tem que sair pelo HTTP do lado da leitura.
                var resposta = get(cliente, stats + "/stats/" + codigo);
                assert resposta.statusCode() == 200 : "/stats deveria responder 200";
                assert resposta.body().contains("\"cliques\":5")
                        : "/stats deveria dizer 5 cliques: " + resposta.body();
                assert resposta.body().contains("\"ultimo\":\"") : "/stats deveria trazer o ultimo clique";

                // Codigo que ninguem clicou responde zero, nao 404.
                assert get(cliente, stats + "/stats/naoexiste").body().contains("\"cliques\":0")
                        : "codigo sem clique deveria dar zero";
            });
        } finally {
            contador.parar();
            try {
                consumo.join(Duration.ofSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            statsHttp.stop(0);
            produtor.close();
        }
    }

    /** O consumo e assincrono: a contagem chega, mas nao instantaneamente. */
    private static long esperarContagem(Contador contador, String codigo, long alvo) {
        long limite = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < limite) {
            if (contador.total(codigo) >= alvo) {
                return contador.total(codigo);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return contador.total(codigo);
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

    private static void comEncurtador(Producer<String, String> produtor, Teste teste) throws IOException {
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
            servidor.stop(1); // 1s para o ultimo clique ser publicado antes de fechar o produtor
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
