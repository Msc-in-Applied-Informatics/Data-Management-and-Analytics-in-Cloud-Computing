import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import java.io.*;
import java.util.*;

public class skeleton {

    public static void main(String[] args) {

        // Σύνδεση με το Redis
        Jedis jedis = new Jedis("localhost", 6379);

        try {
            jedis.sendCommand(
                new ProtocolCommand() {
                    public byte[] getRaw() {
                        return "FT.CREATE".getBytes();
                    }
                },
                "movieIdx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "movie:",
                "SCHEMA",
                "title",
                "TEXT",
                "director",
                "TEXT",
                "year",
                "TEXT"
            );
        } catch (Exception e) {
            // index already exists
        }

        Scanner scanner = new Scanner(System.in);

        System.out.print("Εισάγετε το username σας: ");
        String username = scanner.nextLine();

        while (true) {

            System.out.println("\n--- Μενού Redis Movies ---");
            System.out.println("(I)nsert Movie | (Q)uery | (S)tatistics | e(X)it");
            System.out.print("Επιλογή: ");

            String choice = scanner.nextLine().toUpperCase();

            if (choice.equals("I")) {
                insertMovie(jedis, scanner, username);
            } else if (choice.equals("Q")) {
                queryMovie(jedis, scanner, username);
            } else if (choice.equals("S")) {
                showStatistics(jedis, username);
            } else if (choice.equals("X")) {
                System.out.println("Έξοδος...");
                break;
            } else {
                System.out.println("Μη έγκυρη επιλογή.");
            }
        }

        jedis.close();
        scanner.close();
    }

    public static void insertMovie(Jedis jedis, Scanner scanner, String username) {

        // Title
        System.out.print("Τίτλος: ");
        String title = scanner.nextLine();

        // Director
        System.out.print("Σκηνοθέτης: ");
        String director = scanner.nextLine();

        // Year
        System.out.print("Έτος: ");
        String year = scanner.nextLine();

        String key = title.toLowerCase();

        // Create hashKey for database
        String movieHashKey = "movie:" + key;
        String watchListKey = "watchlist:" + key;

        // Check if already exist the key
        if (jedis.exists(movieHashKey)) {

            jedis.sadd(watchListKey, username);

            System.out.println("Η ταινία προστέθηκε στα watchlist");

        } else {

            Map<String, String> movie = new HashMap<>();

            movie.put("title", title);
            movie.put("director", director);
            movie.put("year", year);

            jedis.hset(movieHashKey, movie);

            jedis.sadd(watchListKey, username);

            System.out.println("Η ταινία καταχωρήθηκε επιτυχώς.");
        }
    }

    public static void queryMovie(Jedis jedis, Scanner scanner, String username) {

        System.out.print("Αναζήτηση Τίτλου: ");

        String title = scanner.nextLine();

        String key = title.toLowerCase();

        String movieHashKey = "movie:" + key;
        String watchListKey = "watchlist:" + key;

        if (jedis.exists(movieHashKey)) {

            Map<String, String> movie = jedis.hgetAll(movieHashKey);

            jedis.sadd(watchListKey, username);

            long length = jedis.scard(watchListKey);

            System.out.println("Βρέθηκε:");
            System.out.println(movie);

            System.out.println("Συνολικό πλήθος χρηστών: " + length);

            // Trending
            jedis.zincrby("movie:trending", 1, movieHashKey);

            // History
            updateHistory(jedis, username, title);

        } else {

            fuzzySearch(jedis, scanner, username, title);
        }
    }

    public static void fuzzySearch(Jedis jedis, Scanner scanner, String username, String title) {

        title = title.toLowerCase();

        String fuzzyQuery = "";

        String[] words = title.split(" ");

        for (String word : words) {
            if(word.equals("the") || word.equals("a") || word.equals("an")) {
                continue;
            }

            fuzzyQuery += "%" + word + "% ";
        }

        fuzzyQuery = fuzzyQuery.trim();

        Object result = jedis.sendCommand(

                new ProtocolCommand() {

                    public byte[] getRaw() {
                        return "FT.SEARCH".getBytes();
                    }
                },

                "movieIdx",
                fuzzyQuery
        );

        List<Object> searchResults = (List<Object>) result;

        long total = (Long) searchResults.get(0);

        if (total > 0) {

            System.out.println("Βρέθηκαν παρόμοιες ταινίες:");

            List<String> movieOptions = new ArrayList<>();

            int option = 1;

            for (int j = 1; j < searchResults.size(); j += 2) {

                String foundMovieKey = new String((byte[]) searchResults.get(j));

                Map<String, String> movie = jedis.hgetAll(foundMovieKey);

                movieOptions.add(foundMovieKey);

                System.out.println(option + ". " + movie.get("title"));

                option++;
            }

            System.out.print("Επιλέξτε αριθμό (0 για ακύρωση): ");

            int selected = Integer.parseInt(scanner.nextLine());

            if (selected > 0 && selected <= movieOptions.size()) {

                String selectedMovieKey = movieOptions.get(selected - 1);

                Map<String, String> movie = jedis.hgetAll(selectedMovieKey);

                String selectedWatchlistKey = "watchlist:" + movie.get("title").toLowerCase();

                jedis.sadd(selectedWatchlistKey, username);

                long length = jedis.scard(selectedWatchlistKey);

                System.out.println("Βρέθηκε:");
                System.out.println(movie);

                System.out.println("Συνολικό πλήθος χρηστών: " + length);

                // Trending
                jedis.zincrby("movie:trending", 1, selectedMovieKey);

                // History
                updateHistory(jedis, username, movie.get("title"));
            }
        } else {
            System.out.println("Η ταινία με τίτλο " + title + " δεν βρέθηκε");
        }
    }

    public static void updateHistory(Jedis jedis, String username, String title) {

        String historyKey = "user:" + username.toLowerCase() + ":history";

        if (jedis.llen(historyKey) == 5) {

            jedis.rpop(historyKey);
        }

        jedis.lpush(historyKey, title);
    }

    public static void showStatistics(Jedis jedis, String username) {

        // Top Ταινίες
        List<String> topMovies = jedis.zrevrange("movie:trending", 0, 2);

        System.out.println("~~~~Top Ταινίες~~~~");

        int i = 1;

        for (String key : topMovies) {
            Map<String, String> movie = jedis.hgetAll(key);
            System.out.println("Top-" + i + " :" + movie);
            i++;
        }

        // User History
        String historyKey = "user:" + username.toLowerCase() + ":history";

        List<String> topSearch = jedis.lrange(historyKey, 0, -1);

        System.out.println("~~~~Top 5 Αναζητήσεις~~~~");

        i = 1;

        for (String searchKey : topSearch) {
            System.out.println("-" + i + ": " + searchKey);
            i++;
        }

        // Popularity Metrics
        System.out.println("~~~~Popularity Metrics~~~~");

        Set<String> watchlists = jedis.keys("watchlist:*");

        for (String key : watchlists) {
            String movie = key.replace("watchlist:", "");
            long users = jedis.scard(key);

            System.out.println(movie + " -> " + users);
        }
    }
}