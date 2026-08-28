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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Fase 3 do encurtador de URLs.
 *
 * A logica das fases 1 e 2 continua igual (codigo deterministico + arquivo).
 * O que muda: agora existe um servidor HTTP ouvindo numa porta, e o encurtador
 * virou site - da para clicar no link e o navegador ser levado ao destino.
 *
 * Servidor: jdk.httpserver, que ja vem no JDK. Sem Spring e sem Maven de
 * proposito - a ideia e ver requisicao, resposta e codigo de status na mao.
 *
 * Ainda atende UMA requisicao por vez (executor padrao do HttpServer).
 * Isso e o assunto da fase 4.
 */
public class Encurtador {

    private final Map<String, String> links = new HashMap<>();
    private final Path arquivo;

    private static final String ALFABETO = "bcdfghjklmnpqrstvwxyz0123456789";
    private static final int TAMANHO_CODIGO = 6;

    public Encurtador(Path arquivo) {
        this.arquivo = arquivo;
        carregar();
    }

    // ---------------------------------------------------------------- dominio

    /** Guarda uma URL e devolve o codigo curto dela. Mesma URL -> mesmo codigo. */
    public String encurtar(String urlOriginal) {
        validar(urlOriginal);
        for (int tentativa = 0; ; tentativa++) {
            String codigo = gerarCodigo(urlOriginal, tentativa);
            String jaGravado = links.get(codigo);
            if (jaGravado == null) {
                links.put(codigo, urlOriginal);
                salvar();
                return codigo;
            }
            if (jaGravado.equals(urlOriginal)) {
                return codigo;
            }
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

    /**
     * Fronteira de confianca: a partir da fase 3 a URL vem de fora, pela rede.
     * Aceitar qualquer texto aqui deixaria o servico redirecionar para coisas
     * como "javascript:..." - um redirect aberto. So http e https passam.
     */
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
     * ponytail: reescreve o arquivo todo a cada link novo. Simples e suficiente
     * ate uns milhares de links; se virar gargalo, troca por banco de dados.
     */
    private void salvar() {
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
     * Sobe o servidor. Porta 0 = "escolha uma porta livre" (o auto-teste usa isso).
     * Quem chama e dono do servidor: e ele que decide quando parar.
     */
    public HttpServer servir(int porta) throws IOException {
        HttpServer servidor = HttpServer.create(new InetSocketAddress(porta), 0);
        servidor.createContext("/", this::rotear);
        servidor.start();
        return servidor;
    }

    /** Decide o que fazer com base em metodo + caminho. E o "roteador". */
    private void rotear(HttpExchange troca) throws IOException {
        String metodo = troca.getRequestMethod();
        String caminho = troca.getRequestURI().getPath();

        if (metodo.equals("POST") && caminho.equals("/encurtar")) {
            postEncurtar(troca);
        } else if (metodo.equals("GET") && caminho.equals("/")) {
            responder(troca, 200, "POST /encurtar com a URL no corpo. GET /<codigo> redireciona.\n");
        } else if (metodo.equals("GET")) {
            getRedirecionar(troca, caminho.substring(1));
        } else {
            responder(troca, 405, "metodo nao suportado aqui\n");
        }
    }

    /** POST /encurtar - corpo e a URL em texto puro. 201 + codigo criado. */
    private void postEncurtar(HttpExchange troca) throws IOException {
        String url = new String(troca.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        try {
            responder(troca, 201, "/" + encurtar(url) + "\n");
        } catch (IllegalArgumentException recusada) {
            responder(troca, 400, recusada.getMessage() + "\n"); // culpa de quem mandou
        }
    }

    /** GET /<codigo> - 301 para o destino, ou 404 se o codigo nao existir. */
    private void getRedirecionar(HttpExchange troca, String codigo) throws IOException {
        String destino = resolver(codigo);
        if (destino == null) {
            responder(troca, 404, "codigo nao encontrado\n");
            return;
        }
        // 301 = "mudou de endereco para sempre". O navegador guarda e nas
        // proximas vezes vai direto, sem passar por aqui.
        troca.getResponseHeaders().set("Location", destino);
        troca.sendResponseHeaders(301, -1); // -1 = resposta sem corpo
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

        System.out.println("Ouvindo em http://localhost:" + porta);
        System.out.println("  curl -X POST --data 'https://www.google.com' http://localhost:" + porta + "/encurtar");
        System.out.println("  curl -i http://localhost:" + porta + "/<codigo>");
        System.out.println("Ctrl+C para parar.");
    }

    // ---------------------------------------------------------------- auto-teste

    /** Sobe um servidor de verdade numa porta livre e conversa com ele por HTTP. */
    private static void autoTeste() throws IOException {
        Path temp = Files.createTempFile("encurtador-teste", ".properties");
        Files.delete(temp);

        Encurtador e = new Encurtador(temp);
        HttpServer servidor = e.servir(0);
        String base = "http://localhost:" + servidor.getAddress().getPort();

        try (HttpClient cliente = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER) // queremos VER o 301
                .build()) {

            var criado = post(cliente, base + "/encurtar", "https://exemplo.com");
            assert criado.statusCode() == 201 : "criar deveria dar 201, deu " + criado.statusCode();
            String codigo = criado.body().trim();
            assert codigo.startsWith("/") && codigo.length() == TAMANHO_CODIGO + 1 : "codigo estranho: " + codigo;

            // Idempotencia continua valendo, agora pela rede.
            assert codigo.equals(post(cliente, base + "/encurtar", "https://exemplo.com").body().trim())
                    : "mesma URL deveria dar o mesmo codigo";
            assert e.quantidade() == 1 : "nao deveria ter duplicado";

            var redirect = get(cliente, base + codigo);
            assert redirect.statusCode() == 301 : "deveria redirecionar com 301, deu " + redirect.statusCode();
            assert redirect.headers().firstValue("Location").orElse("").equals("https://exemplo.com")
                    : "Location errado";

            assert get(cliente, base + "/naoexiste").statusCode() == 404 : "codigo inexistente deveria dar 404";
            assert post(cliente, base + "/encurtar", "").statusCode() == 400 : "URL vazia deveria dar 400";
            assert post(cliente, base + "/encurtar", "javascript:alert(1)").statusCode() == 400
                    : "esquema perigoso deveria dar 400";
            assert get(cliente, base + "/").statusCode() == 200 : "raiz deveria explicar o uso";

        } catch (InterruptedException interrompido) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrompido);
        } finally {
            servidor.stop(0);
            Files.deleteIfExists(temp);
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
