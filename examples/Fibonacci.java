import java.util.Map;

public class Fibonacci {
    public Object handler(Map<String, Object> params) {
        int n = 10; // Valore di default

        // Estrazione e casting dinamico del parametro
        if (params != null && params.containsKey("n")) {
            Object val = params.get("n");
            if (val instanceof String) {
                n = Integer.parseInt((String) val);
            } else if (val instanceof Double) {
                n = ((Double) val).intValue();
            }
        }

        // Calcolo della sequenza
        if (n <= 0) return 0;
        if (n == 1) return 1;

        long a = 0;
        long b = 1;
        for (int i = 2; i <= n; i++) {
            long temp = a + b;
            a = b;
            b = temp;
        }
        return b;
    }
}