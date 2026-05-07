import redis.clients.jedis.Jedis;
import java.io.*;
import java.util.*;

public class skeleton {
    public static void main(String[] args) {
        // Σύνδεση με το Redis
        Jedis jedis = new Jedis("localhost", 6379);
        
        Scanner scanner = new Scanner(System.in);
        System.out.print("Εισάγετε το username σας: ");
        String username = scanner.nextLine();
        
        while (true) {
            System.out.println("\n--- Μενού Redis Movies ---");
            System.out.println("(I)nsert Artist | (Q)uery | (S)tatistics | e(X)it");
            System.out.print("Επιλογή: ");
            String choice = scanner.nextLine().toUpperCase();
            
            if (choice.equals("I")) {
                
                System.out.print("Τίτλος: ");
                String title = scanner.nextLine();

                System.out.print("Σκηνοθέτης: ");
                String director = scanner.nextLine();

                System.out.print("Έτος: ");
                String year = scanner.nextLine();

                String key = title.toLowerCase();
                
                String movieHashKey = "movie:" + key;
                String watchListKey = "watchlist:" + key;

                if(jedis.exists(key)){
                    jedis.sadd(watchListKey, username);
                    System.out.println("Η ταινία προστέθηκε στα watchlist");
                }else{
                    Map<String,String> movie = new HashMap<>();

                    movie.put("title", title);
                    movie.put("director", director);
                    movie.put("year", year);

                    jedis.hset(movieHashKey, movie);
                    jedis.sadd(watchListKey, username);

                    System.out.println("Η ταινία καταχωρήθηκε επιτυχώς.");
                }
                
            } else if (choice.equals("Q")) {
                // TODO: Υλοποίηση αναζήτησης
            } else if (choice.equals("S")) {
                // TODO: Υλοποίηση στατιστικών
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
}
