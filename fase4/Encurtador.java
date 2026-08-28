import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Fase 4 do encurtador de URLs.
 *
 * Mesma coisa da fase 3, mas agora atende MUITA gente ao mesmo tempo.
 * Tres mudancas, e as tres precisam andar juntas:
 *
 *   1. o servidor ganhou um executor de virtual threads - uma thread leve por
 *      requisicao, e o JDK aguenta milhoes delas;
 *   2. o HashMap virou ConcurrentHashMap - com varias threads escrevendo no
 *      mesmo mapa, o HashMap corrompe silenciosamente;
 *   3. encurtar deixou de "olhar e depois gravar" (que da corrida entre as duas
 *      coisas) e passou a usar putIfAbsent, que faz as duas num passo so.
 *
 * Trocar so o executor e um jeito muito bom de criar bug dificil de achar.
 */
public class Encurtador {

    /** Agora concorrente: varias virtual threads leem e escrevem aqui ao mesmo tempo. */
    private final Map<String, String> links = new ConcurrentHashMap<>();
    private final Path arquivo;

    private static final String ALFABETO = "bcdfghjklmnpqrstvwxyz0123456789";
    private static final int TAMANHO_CODIGO = 6;

    public Encurtador(Path arquivo) {
        this.arquivo = arquivo;
        carregar();
    }

    // ---------------------------------------------------------------- dominio

    /**
     * Guarda uma URL e devolve o codigo curto dela. Mesma URL -> mesmo codigo.
     *
     * putIfAbsent e atomico: ou grava e devolve null, ou nao grava e devolve o
     * que ja estava la. Com "if (get == null) put" duas threads poderiam passar
     * pelo if juntas e uma sobrescrever a outra.
     */
    public String encurtar(String urlOriginal) {
        validar(urlOriginal);
        for (int tentativa = 0; ; tentativa++) {
            String codigo = gerarCodigo(urlOriginal, tentativa);
            String jaGravado = links.putIfAbsent(codigo, urlOriginal);
            if (jaGravado == null) {
                salvar();
                return codigo; // era nosso: gravamos agora
            }
            if (jaGravado.equals(urlOriginal)) {
                return codigo; // outra thread ja gravou a mesma URL: perfeito
            }
            // codigo ocupado por OUTRA URL: colisao, tenta o proximo
        }
    }

    /** Devolve a URL original de um codigo, ou null se o codigo nao existir. */
    public String resolver(String codigo) {
        return links.get(codigo);
    }

    /** Quantos links estao guardados. */
    public int quantidade() {
        return links.size();
    }

    /** So http e https - senao o servico redireciona para "javascript:..." (open redirect). */
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
     * synchronized: sem isso, duas threads reescrevendo o arquivo ao mesmo tempo
     * deixam ele pela metade. O mapa e concorrente, o arquivo nao.
     *
     * ponytail: trava global + reescrita do arquivo inteiro a cada link novo.
     * E o gargalo de escrita desta fase, de proposito - vira banco de dados
     * quando incomodar. Leitura (o redirect) nao passa por aqui e nao trava.
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

    // ----------------------------------------------------------------- servidor

    /**
     * Sobe o servidor com uma virtual thread por requisicao.
     *
     * Thread do sistema operacional custa ~1MB de pilha: alguns milhares e o
     * limite. Virtual thread custa alguns bytes e, quando bloqueia esperando
     * I/O, sai da frente e devolve a thread real para outra requisicao usar.
     * Por isso da para ter milhoes.
     */
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

    /**
     * Rota so para enxergar o efeito: finge um trabalho lento de I/O (1 segundo).
     * Dispare 100 ao mesmo tempo - todas terminam em ~1s, nao em 100s.
     * Na fase 3 (uma por vez) isso levaria 100 segundos.
     */
    private void lento(HttpExchange troca) throws IOException {
        try {
            Thread.sleep(1000); // numa virtual thread isso nao segura thread do SO
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

    private void getRedirecionar(HttpExchange troca, String codigo) throws IOException {
        String destino = resolver(codigo);
        if (destino == null) {
            responder(troca, 404, "codigo nao encontrado\n");
            return;
        }
        troca.getResponseHeaders().set("Location", destino);
        troca.sendResponseHeaders(301, -1);
        troca.close();
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
        autoTeste();
        System.out.println("Auto-teste: OK");

        int porta = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        new Encurtador(Path.of("links.properties")).servir(porta);

        System.out.println("Ouvindo em http://localhost:" + porta + " (uma virtual thread por requisicao)");
        System.out.println("  curl -X POST --data 'https://www.google.com' http://localhost:" + porta + "/encurtar");
        System.out.println("  curl -i http://localhost:" + porta + "/<codigo>");
        System.out.println("  100 lentas ao mesmo tempo, todas em ~1s:");
        System.out.println("    seq 100 | xargs -P100 -I{} curl -s -o /dev/null http://localhost:" + porta + "/lento");
        System.out.println("Ctrl+C para parar.");
    }

    // ---------------------------------------------------------------- auto-teste

    private static void autoTeste() throws IOException {
        Path temp = Files.createTempFile("encurtador-teste", ".properties");
        Files.delete(temp);

        Encurtador e = new Encurtador(temp);
        HttpServer servidor = e.servir(0);
        String base = "http://localhost:" + servidor.getAddress().getPort();

        try (HttpClient cliente = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .build()) {

            // --- o mesmo contrato da fase 3 continua valendo ---
            var criado = post(cliente, base + "/encurtar", "https://exemplo.com");
            assert criado.statusCode() == 201 : "criar deveria dar 201, deu " + criado.statusCode();
            String codigo = criado.body().trim();

            var redirect = get(cliente, base + codigo);
            assert redirect.statusCode() == 301 : "deveria redirecionar com 301";
            assert redirect.headers().firstValue("Location").orElse("").equals("https://exemplo.com")
                    : "Location errado";

            assert get(cliente, base + "/naoexiste").statusCode() == 404 : "inexistente deveria dar 404";
            assert post(cliente, base + "/encurtar", "").statusCode() == 400 : "URL vazia deveria dar 400";
            assert post(cliente, base + "/encurtar", "javascript:alert(1)").statusCode() == 400
                    : "esquema perigoso deveria dar 400";

            // --- o que e novo na fase 4 ---

            // 200 URLs distintas gravadas ao mesmo tempo: nenhuma pode se perder.
            // Com HashMap no lugar do ConcurrentHashMap, isto falha (as vezes).
            int n = 200;
            List<Future<String>> criacoes = new ArrayList<>();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < n; i++) {
                    String url = "https://exemplo.com/pagina/" + i;
                    criacoes.add(pool.submit(() -> post(cliente, base + "/encurtar", url).body().trim()));
                }
            }
            assert e.quantidade() == n + 1 : "deveria ter " + (n + 1) + " links, tem " + e.quantidade();

            // A mesma URL disputada por 50 threads tem que dar UM codigo so.
            List<Future<String>> corrida = new ArrayList<>();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 50; i++) {
                    corrida.add(pool.submit(() -> post(cliente, base + "/encurtar", "https://disputada.com").body().trim()));
                }
            }
            String primeiro = valor(corrida.get(0));
            for (Future<String> f : corrida) {
                assert primeiro.equals(valor(f)) : "corrida gerou codigos diferentes para a mesma URL";
            }
            assert e.quantidade() == n + 2 : "a URL disputada deveria ter virado 1 link so";

            // 50 requisicoes de 1s cada, ao mesmo tempo. Uma por vez levaria 50s.
            long inicio = System.nanoTime();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 50; i++) {
                    pool.submit(() -> get(cliente, base + "/lento"));
                }
            }
            long segundos = (System.nanoTime() - inicio) / 1_000_000_000L;
            assert segundos < 10 : "50 requisicoes de 1s levaram " + segundos + "s - nao ficaram concorrentes";

        } catch (InterruptedException interrompido) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrompido);
        } finally {
            servidor.stop(0);
            Files.deleteIfExists(temp);
        }
    }

    private static String valor(Future<String> f) {
        try {
            return f.get();
        } catch (Exception e) {
            throw new AssertionError("requisicao concorrente falhou", e);
        }
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
}
