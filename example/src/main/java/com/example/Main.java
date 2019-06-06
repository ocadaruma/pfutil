package com.example;

public class Main {
    public static void main(String[] args) throws Exception {
        LettuceRedisClient redisClient = new LettuceRedisClient("127.0.0.1", 6379);

        // count unique visitor per domain using Redis
        redisClient.pfAdd("domain1.com", "user1", "user2", "user3");
        redisClient.pfAdd("domain2.com", "user1", "user2", "user3", "user4");

        // count unique visitor per domain using embedded HLL
        EmbeddedCardinalityEstimator domain3Estimator = new EmbeddedCardinalityEstimator();
        domain3Estimator.add("user1", "user3", "user5");

        EmbeddedCardinalityEstimator domain4Estimator = new EmbeddedCardinalityEstimator();
        domain4Estimator.add("user2", "user4", "user6");

        // create new estimator then merge all HLLs into it on memory
        EmbeddedCardinalityEstimator all = new EmbeddedCardinalityEstimator();
        all.merge(redisClient.get("domain1.com"));
        all.merge(redisClient.get("domain2.com"));
        all.merge(domain3Estimator.dump());
        all.merge(domain4Estimator.dump());

        // write back HLL into Redis
        redisClient.set("all", all.dump());

        // show resulting count by Redis
        // unique visitor count will be 6
        System.out.print("Overall unique visitor count: ");
        System.out.println(redisClient.pfCount("all"));

        redisClient.close();
    }
}
