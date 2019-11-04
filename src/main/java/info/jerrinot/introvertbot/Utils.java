package info.jerrinot.introvertbot;

public final class Utils {
    private Utils() {

    }

    public static Long countPersons(Frame e) {
        return e.getObjects().stream().filter(o -> o.getName().equals("person")).count();
    }
}
