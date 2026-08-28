import java.util.HashMap;
import java.util.Map;

/**
 * Fase 1 do encurtador de URLs.
 *
 * Ainda nao tem servidor, nem Kafka, nem nada em rede: e um programa que roda,
 * faz o trabalho na memoria do computador, imprime o resultado e termina.
 * O objetivo aqui e so entender a forma de um programa Java.
 */
public class Encurtador {

    /**
     * A "tabela" que guarda os links. A chave e o codigo curto ("a1b2c3"),
     * o valor e a URL original. Vive so na memoria: fecha o programa, perde tudo.
     */
    private final Map<String, String> links = new HashMap<>();

    /** Caracteres permitidos no codigo curto. Sem vogais, para nao formar palavras feias. */
    private static final String ALFABETO = "bcdfghjklmnpqrstvwxyz0123456789";

    /** Guarda uma URL e devolve o codigo curto criado para ela. */
    public String encurtar(String urlOriginal) {
        if (urlOriginal == null || urlOriginal.isBlank()) {
            throw new IllegalArgumentException("URL vazia");
        }
        String codigo = gerarCodigo();
        links.put(codigo, urlOriginal);
        return codigo;
    }

    /** Devolve a URL original de um codigo, ou null se o codigo nao existir. */
    public String resolver(String codigo) {
        return links.get(codigo);
    }

    /** Quantos links estao guardados. */
    public int quantidade() {
        return links.size();
    }

    /** Sorteia um codigo de 6 caracteres que ainda nao esteja em uso. */
    private String gerarCodigo() {
        String codigo;
        do {
            StringBuilder sb = new StringBuilder(6);
            for (int i = 0; i < 6; i++) {
                int posicao = (int) (Math.random() * ALFABETO.length());
                sb.append(ALFABETO.charAt(posicao));
            }
            codigo = sb.toString();
        } while (links.containsKey(codigo)); // se ja existir, sorteia de novo
        return codigo;
    }

    /** Ponto de entrada: e por aqui que o Java comeca a executar. */
    public static void main(String[] args) {
        Encurtador app = new Encurtador();

        String codigo = app.encurtar("https://www.google.com");
        System.out.println("Encurtado: https://www.google.com -> /" + codigo);
        System.out.println("Resolvido: /" + codigo + " -> " + app.resolver(codigo));

        autoTeste();
        System.out.println("Auto-teste: OK");
    }

    /** Checagem minima: se a logica quebrar, o programa para aqui com erro. */
    private static void autoTeste() {
        Encurtador e = new Encurtador();

        String c = e.encurtar("https://exemplo.com");
        assert c.length() == 6 : "codigo deveria ter 6 caracteres";
        assert "https://exemplo.com".equals(e.resolver(c)) : "deveria devolver a URL original";
        assert e.resolver("naoexiste") == null : "codigo inexistente deveria dar null";
        assert e.quantidade() == 1 : "deveria ter 1 link guardado";

        // Duas URLs iguais geram codigos diferentes (por enquanto - resolvemos isso na fase 2).
        String c2 = e.encurtar("https://exemplo.com");
        assert !c.equals(c2) : "codigos deveriam ser diferentes";
        assert e.quantidade() == 2 : "deveria ter 2 links guardados";

        try {
            e.encurtar("");
            throw new AssertionError("URL vazia deveria ter sido recusada");
        } catch (IllegalArgumentException esperado) {
            // certo: recusou como devia
        }
    }
}
