package listeningpost;

import java.util.function.Function;

/** SBS-1 BaseStation line parser, a port of internal/processor/sbs.go: positional, identical for every message type. */
final class Sbs {
    static final int FIELDS = 22;

    private Sbs() {}

    /** Parses one line into the SBS fields of a Decoded; envelope fields are left for the caller. */
    static Decoded parse(String raw) {
        String[] f = raw.split(",", -1);
        if (f.length != FIELDS) {
            throw new IllegalArgumentException("sbs: " + f.length + " fields, want " + FIELDS);
        }
        Decoded m = new Decoded();
        m.messageType = f[0];
        m.transmissionType = opt("transmission_type", f[1], Integer::parseInt);
        m.icao = blankToNull(f[4]);
        m.generated = joinDateTime(f[6], f[7]);
        m.logged = joinDateTime(f[8], f[9]);
        m.callsign = blankToNull(f[10].strip());
        m.altitude = opt("altitude", f[11], Integer::parseInt);
        m.groundSpeed = opt("ground_speed", f[12], Double::parseDouble);
        m.track = opt("track", f[13], Double::parseDouble);
        m.lat = opt("lat", f[14], Double::parseDouble);
        m.lon = opt("lon", f[15], Double::parseDouble);
        m.verticalRate = opt("vertical_rate", f[16], Integer::parseInt);
        m.squawk = blankToNull(f[17]);
        m.alert = opt("alert", f[18], Sbs::flag);
        m.emergency = opt("emergency", f[19], Sbs::flag);
        m.spi = opt("spi", f[20], Sbs::flag);
        m.onGround = opt("on_ground", f[21], Sbs::flag);
        return m;
    }

    private static <T> T opt(String name, String s, Function<String, T> parse) {
        if (s.isEmpty()) {
            return null;
        }
        try {
            return parse.apply(s);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("sbs: " + name + " \"" + s + "\": " + e.getMessage(), e);
        }
    }

    /** SBS booleans are -1 (true) or 0 (false). */
    private static boolean flag(String s) {
        return switch (s) {
            case "-1" -> true;
            case "0" -> false;
            default -> throw new IllegalArgumentException("not -1 or 0");
        };
    }

    private static String blankToNull(String s) {
        return s.isEmpty() ? null : s;
    }

    private static String joinDateTime(String date, String clock) {
        return date.isEmpty() && clock.isEmpty() ? null : date + " " + clock;
    }
}
