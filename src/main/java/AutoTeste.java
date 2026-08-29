import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * Auto-teste da fase 7.
 *
 * O teste que define a fase e {@link #duasInstancias()}: sobe DUAS copias do
 * Encurtador, encurta numa e resolve na outra. Ate a fase 6 isso dava 404, e o
 * 404 era a prova de que o servidor tinha estado dentro dele. Passar agora e a
 * prova de que nao tem mais - que e a definicao de stateless, e a unica coisa
 * que o load balancer da fase 7 exige do nosso lado.
 *
 * Repare na assimetria das dependencias: sem Kafka os testes rodam (so pulam a
 * parte da contagem), sem Postgres nao roda nada. E o esperado - o banco e a
 * fonte da verdade, o broker e estatistica.
 */
final class AutoTeste {

    /**
     * Prefixo das URLs desta execucao, unico por processo.
     *
     * Existe porque o banco agora e compartilhado: as duas instancias sobem
     * juntas (o compose nao as enfileira) e cada uma roda o proprio auto-teste
     * contra a MESMA tabela. Com um prefixo fixo, a limpeza de uma apagava as
     * linhas da outra no meio do teste - e o sintoma era um clique respondendo
     * 404 sem nada estar quebrado no codigo testado.
     */
    private static final String MARCA = "https://exemplo.com/teste-" + UUID.randomUUID();

    static void rodar(String bootstrap) throws IOException {
        exigirBanco();
        contratoHttp();
        duasInstancias();
        health();
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
            var criado = post(cliente, base + "/encurtar", MARCA);
            assert criado.statusCode() == 201 : "criar deveria dar 201";
            String codigo = criado.body().trim();

            var redirect = get(cliente, base + codigo);
            assert redirect.statusCode() == 302 : "deveria redirecionar com 302";
            assert redirect.headers().firstValue("Location").orElse("").equals(MARCA)
                    : "Location errado";

            assert get(cliente, base + "/naoexiste").statusCode() == 404 : "inexistente deveria dar 404";
            assert post(cliente, base + "/encurtar", "").statusCode() == 400 : "URL vazia deveria dar 400";
            assert post(cliente, base + "/encurtar", "javascript:alert(1)").statusCode() == 400
                    : "esquema perigoso deveria dar 400";
        });
    }

    /**
     * O teste da fase 7: encurtar numa instancia e resolver em OUTRA.
     *
     * Sao dois objetos Encurtador independentes, em portas diferentes, sem
     * nenhuma referencia entre si - exatamente como seriam dois processos atras
     * do Nginx. O unico caminho entre eles e o banco.
     *
     * Ate a fase 6 este teste falhava com 404, e falhava por um motivo bom: o
     * link so existia na memoria de quem o criou.
     */
    private static void duasInstancias() throws IOException {
        Encurtador a = novoEncurtador(null);
        Encurtador b = novoEncurtador(null);
        HttpServer servidorA = a.servir(0);
        HttpServer servidorB = b.servir(0);
        try (HttpClient cliente = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER).build()) {
            String baseA = "http://localhost:" + servidorA.getAddress().getPort();
            String baseB = "http://localhost:" + servidorB.getAddress().getPort();
            String url = MARCA + "/instancias/" + UUID.randomUUID();

            String codigo = post(cliente, baseA + "/encurtar", url).body().trim();

            var naOutra = get(cliente, baseB + codigo);
            assert naOutra.statusCode() == 302
                    : "instancia B deveria resolver o que A criou, veio " + naOutra.statusCode();
            assert naOutra.headers().firstValue("Location").orElse("").equals(url)
                    : "instancia B redirecionou para o lugar errado";

            // E o caminho de volta: encurtar a MESMA URL em B da o mesmo codigo,
            // sem criar linha nova - hash deterministico (fase 2) + chave primaria.
            assert post(cliente, baseB + "/encurtar", url).body().trim().equals(codigo)
                    : "a mesma URL deveria dar o mesmo codigo em qualquer instancia";
        } catch (InterruptedException interrompido) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrompido);
        } finally {
            servidorA.stop(0);
            servidorB.stop(0);
            limpar();
        }
    }

    /** O que o load balancer pergunta antes de mandar trafego. */
    private static void health() throws IOException {
        comEncurtador(null, (app, base, cliente) -> {
            var resposta = get(cliente, base + "/health");
            assert resposta.statusCode() == 200 : "/health deveria dar 200 com o banco de pe";
            assert resposta.headers().firstValue("X-Instancia").isPresent()
                    : "toda resposta deveria dizer qual instancia respondeu";
        });
    }

    /** Kafka fora do ar nao pode derrubar o redirect. */
    private static void degradaSemKafka() throws IOException {
        System.out.println("  (os WARN de conexao a seguir sao o teste: broker morto de proposito)");
        Producer<String, String> produtorQuebrado = Encurtador.produtorPadrao("localhost:9099");
        try {
            comEncurtador(produtorQuebrado, (app, base, cliente) -> {
                String codigo = post(cliente, base + "/encurtar", MARCA).body().trim();
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
        // Grupo proprio: um Contador de verdade rodando na maquina dividiria as
        // particoes com o teste, e ele veria so parte dos proprios cliques.
        String grupoDoTeste = "teste-" + UUID.randomUUID();
        Thread consumo = Thread.ofVirtual().start(() -> contador.consumir(bootstrap, grupoDoTeste));

        Producer<String, String> produtor = Encurtador.produtorPadrao(bootstrap);
        try {
            String stats = "http://localhost:" + statsHttp.getAddress().getPort();
            comEncurtador(produtor, (app, base, cliente) -> {
                String url = MARCA + "/contagem/" + UUID.randomUUID();
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
        Encurtador app = novoEncurtador(produtor);
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
            limpar();
        }
    }

    private static Encurtador novoEncurtador(Producer<String, String> produtor) {
        return new Encurtador(Encurtador.DB_URL, Encurtador.DB_USER, Encurtador.DB_PASSWORD, produtor);
    }

    /**
     * O banco e dependencia dura: sem ele nao ha o que testar, e falhar aqui com
     * a instrucao na tela e melhor que falhar la na frente com um stack trace de
     * conexao recusada.
     */
    private static void exigirBanco() {
        try (Connection c = DriverManager.getConnection(
                Encurtador.DB_URL, Encurtador.DB_USER, Encurtador.DB_PASSWORD)) {
            assert c.isValid(2) : "conexao invalida";
        } catch (SQLException fora) {
            throw new IllegalStateException("Nao conectei em " + Encurtador.DB_URL
                    + " como '" + Encurtador.DB_USER + "'. Para preparar o banco:\n"
                    + "  sudo -u postgres psql -c \"CREATE ROLE " + Encurtador.DB_USER + " LOGIN CREATEDB\""
                    + " -c \"CREATE DATABASE encurtador OWNER " + Encurtador.DB_USER + "\"\n"
                    + "  psql -d encurtador -c \"ALTER ROLE " + Encurtador.DB_USER + " PASSWORD 'algo'\"\n"
                    + "  export DB_PASSWORD=algo\n"
                    + "(pelo socket o psql usa peer e nao pede senha; o JDBC vai por TCP e pede)", fora);
        }
    }

    /** Os testes gravam de verdade; some com o que eles criaram. */
    private static void limpar() {
        try (Connection c = DriverManager.getConnection(
                     Encurtador.DB_URL, Encurtador.DB_USER, Encurtador.DB_PASSWORD);
             var del = c.prepareStatement("DELETE FROM links WHERE url LIKE ?")) {
            del.setString(1, MARCA + "%");
            del.executeUpdate();
        } catch (SQLException e) {
            System.err.println("limpeza do teste falhou: " + e.getMessage());
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
