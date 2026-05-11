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
            System.out.println("(I)nsert Movie | (Q)uery | (S)tatistics | e(X)it");
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

                if(jedis.exists(movieHashKey)){
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
                System.out.print("Αναζήτηση Τίτλου: ");
                String title = scanner.nextLine();
                String key = title.toLowerCase();
                String movieHashKey = "movie:" + key;
                String watchListKey = "watchlist:" + key;

                if(jedis.exists(movieHashKey)){
                    Map<String, String> movie = jedis.hgetAll(movieHashKey);
                    jedis.sadd(watchListKey, username);

                    long length = jedis.scard(watchListKey);
                    System.out.println("Βρέθηκε: ");                    
                    System.out.println(movie);

                    System.out.println("Συνολικό πλήθος χρηστών: " + length);
                    
                    // Trending
                    jedis.zincrby("movie:trending", 1, movieHashKey);
                    // History
                    String historyKey = "user:" + username.toLowerCase() + ":history";
                    if(jedis.llen(historyKey) == 5) 
                        jedis.rpop(historyKey);
                    
                    jedis.lpush(historyKey, title);

                    
                }else{
                    System.out.println("Η ταινία με τίτλο " + title + " δεν βρέθηκε" );
                }
                
            } else if (choice.equals("S")) {
                // Top Ταινίες
                List<String> topMovies = jedis.zrevrange("movie:trending", 0, 2);
                System.out.println("~~~~Top Ταινίες~~~~");
                int i = 1;
                for(String key : topMovies){
                    Map<String, String> movie = jedis.hgetAll(key);
                    System.out.println("Top-" + i +" :"  + movie);
                    i++;
                }
                // User History
                String historyKey = "user:" + username.toLowerCase() + ":history";
                List<String> topSearch = jedis.lrange(historyKey, 0 , -1);
                System.out.println("~~~~Top 5 Αναζητήσεις~~~~");
                i = 1;
                for(String searchKey : topSearch){
                    System.out.println("-"+ i + ": " + searchKey);
                    i++; 
                } 
                // Popularity Metrics
                System.out.println("~~~~Popularity Metrics~~~~");
                Set<String> watchlists = jedis.keys("watchlist:*");
                for(String key: watchlists){
                    String movie = key.replace("watchlist:","");
                    long users = jedis.scard(key);
                    System.out.println( movie + " -> " + users);
                }
                      
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
