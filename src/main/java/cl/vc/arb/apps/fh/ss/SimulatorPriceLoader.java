package cl.vc.arb.apps.fh.ss;

import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads reference prices for the market data simulator from a JSON file.
 * If a file path is provided via the {@code simulador.prices.file} property it
 * is read from the filesystem; otherwise the bundled
 * {@code /simulator-prices.json} classpath resource is used.
 */
@Slf4j
public class SimulatorPriceLoader {

    public static Map<String, SimulatorPriceRef> load(String filePath) {
        Map<String, SimulatorPriceRef> map = new LinkedHashMap<>();
        try {
            JSONParser parser = new JSONParser();
            Object obj;

            if (filePath != null && !filePath.isBlank()) {
                log.info("simulator loading prices from file={}", filePath);
                obj = parser.parse(new FileReader(filePath));
            } else {
                log.info("simulator loading prices from classpath /simulator-prices.json");
                InputStream is = SimulatorPriceLoader.class.getResourceAsStream("/simulator-prices.json");
                if (is == null) {
                    log.error("simulator classpath resource /simulator-prices.json not found");
                    return map;
                }
                obj = parser.parse(new InputStreamReader(is));
            }

            JSONObject root = (JSONObject) obj;
            JSONArray instruments = (JSONArray) root.get("instruments");

            for (Object item : instruments) {
                JSONObject inst = (JSONObject) item;
                SimulatorPriceRef ref = new SimulatorPriceRef();
                ref.setSymbol((String) inst.get("symbol"));
                ref.setLast(toDouble(inst.get("last")));
                ref.setOpen(toDouble(inst.get("open")));
                ref.setVariation(toDouble(inst.get("variation")));
                ref.setBidQty(toDouble(inst.get("bidQty")));
                ref.setAskQty(toDouble(inst.get("askQty")));
                map.put(ref.getSymbol(), ref);
            }

            log.info("simulator loaded {} reference prices", map.size());
        } catch (Exception e) {
            log.error("simulator failed to load prices", e);
        }
        return map;
    }

    private static double toDouble(Object value) {
        if (value == null) return 0.0;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(value.toString());
    }
}
