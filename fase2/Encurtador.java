import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Fase 2 do encurtador de URLs.
 *
 * Duas mudancas em cima da fase 1:
 *   1. o codigo agora e DETERMINISTICO - a mesma URL sempre vira o mesmo codigo,
 *      entao encurtar duas vezes nao desperdica codigo (idempotencia);
 *   2. os links sobrevivem ao desligamento - ficam num arquivo em disco.
 *
 * Continua sem servidor e sem Kafka: isso e fase 3 e 5.
 */
public class Encurtador {

    /** Codigo curto -> URL original. Espelho em memoria do que esta no arquivo. */
    private final Map<String, String> links = new HashMap<>();

    /** Onde o mapa e gravado. Cada linha do arquivo e "codigo=url". */
    private final Path arquivo;

    /** Caracteres permitidos no codigo curto. Sem vogais, para nao formar palavras feias. */
    private static final String ALFABETO = "bcdfghjklmnpqrstvwxyz0123456789";

    private static final int TAMANHO_CODIGO = 6;

    public Encurtador(Path arquivo) {
        this.arquivo = arquivo;
        carregar();
    }

    /**
     * Guarda uma URL e devolve o codigo curto dela.
     * Chamar duas vezes com a mesma URL devolve o mesmo codigo e nao grava nada de novo.
     */
    public String encurtar(String urlOriginal) {
        if (urlOriginal == null || urlOriginal.isBlank()) {
            throw new IllegalArgumentException("URL vazia");
        }
        // Tentativa 0 e o codigo "natural" da URL. So mudamos de tentativa se
        // esse codigo ja estiver ocupado por OUTRA URL (colisao de hash).
        for (int tentativa = 0; ; tentativa++) {
            String codigo = gerarCodigo(urlOriginal, tentativa);
            String jaGravado = links.get(codigo);
            if (jaGravado == null) {
                links.put(codigo, urlOriginal);
                salvar();
                return codigo;
            }
            if (jaGravado.equals(urlOriginal)) {
                return codigo; // ja estava la, nada a fazer
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
     * Transforma a URL em codigo por hash: mesma entrada, mesma saida, sempre.
     * SHA-256 devolve 32 bytes; usamos os 6 primeiros, um por caractere do codigo.
     * A "tentativa" entra na conta so para desempatar colisao.
     */
    private static String gerarCodigo(String url, int tentativa) {
        byte[] hash = sha256(url + "#" + tentativa);
        StringBuilder sb = new StringBuilder(TAMANHO_CODIGO);
        for (int i = 0; i < TAMANHO_CODIGO; i++) {
            int posicao = (hash[i] & 0xff) % ALFABETO.length();
            sb.append(ALFABETO.charAt(posicao));
        }
        return sb.toString();
    }

    private static byte[] sha256(String texto) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(texto.getBytes());
        } catch (NoSuchAlgorithmException impossivel) {
            throw new IllegalStateException(impossivel); // SHA-256 existe em toda JVM
        }
    }

    /** Le o arquivo para a memoria. Arquivo inexistente = comeco do zero, nao e erro. */
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
     * Grava a memoria inteira no arquivo.
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

    /** Ponto de entrada: e por aqui que o Java comeca a executar. */
    public static void main(String[] args) {
        Encurtador app = new Encurtador(Path.of("links.properties"));

        String codigo = app.encurtar("https://www.google.com");
        System.out.println("Encurtado: https://www.google.com -> /" + codigo);
        System.out.println("Resolvido: /" + codigo + " -> " + app.resolver(codigo));
        System.out.println("Links guardados: " + app.quantidade());

        autoTeste();
        System.out.println("Auto-teste: OK");
    }

    /** Checagem minima: se a logica quebrar, o programa para aqui com erro. */
    private static void autoTeste() {
        Path temp;
        try {
            temp = Files.createTempFile("encurtador-teste", ".properties");
            Files.delete(temp); // queremos so o nome; o arquivo nasce no primeiro salvar()
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try {
            Encurtador e = new Encurtador(temp);

            String c = e.encurtar("https://exemplo.com");
            assert c.length() == TAMANHO_CODIGO : "codigo deveria ter 6 caracteres";
            assert "https://exemplo.com".equals(e.resolver(c)) : "deveria devolver a URL original";
            assert e.resolver("naoexiste") == null : "codigo inexistente deveria dar null";
            assert e.quantidade() == 1 : "deveria ter 1 link guardado";

            // A diferenca da fase 1: mesma URL -> mesmo codigo, sem gravar de novo.
            assert c.equals(e.encurtar("https://exemplo.com")) : "mesma URL deveria dar o mesmo codigo";
            assert e.quantidade() == 1 : "nao deveria ter duplicado";

            // URLs diferentes continuam dando codigos diferentes.
            assert !c.equals(e.encurtar("https://outro.com")) : "URLs diferentes deveriam diferir";
            assert e.quantidade() == 2 : "deveria ter 2 links guardados";

            // O que foi gravado sobrevive: outro objeto, mesmo arquivo, mesma resposta.
            Encurtador reaberto = new Encurtador(temp);
            assert "https://exemplo.com".equals(reaberto.resolver(c)) : "deveria ter persistido";
            assert reaberto.quantidade() == 2 : "deveria ter recarregado os 2 links";

            try {
                e.encurtar("");
                throw new AssertionError("URL vazia deveria ter sido recusada");
            } catch (IllegalArgumentException esperado) {
                // certo: recusou como devia
            }
        } finally {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignorado) {
                // teste ja passou; arquivo temporario perdido nao e problema
            }
        }
    }
}
