

import java.io.*;
import java.util.*;

/*
 * Fatima AlSaadeh
 * Product Recommendation System
 *  A-priori algorithm to find products which are frequently browsed together.
 *  Fix the support to s = 100 (i.e. product pairs need to occur together at least 100 times to be considered frequent)
 *  and find itemsets of size 2 and 3.
 * */
public class question_2 {

    public static void main(String[] args) {
        /*
        *
        * 1- Apriori algorithm start, in the first pass , process the file, find all the items in the transactions, count the occurrence
        * and when all lines "transactions" are processed prune the items that are not frequent "count<100"
        * 2- In the second pass, process the file again find all the pairs of items in the transactions, if every single item in the pair is frequent
        * "in the first pass count" then add it or increase its count if it appears before, prune the pairs that are not frequent "count<100"
        * 3- In the third pass process the file again, find a single item that is in the first pass count "frequent" and then a pair that is in
        * in the second pass count "frequent" and add it or increase its count.
        * 4- find all the pairs confidence value Conf(X->Y)= freq(X,Y)/freq(X), Conf(Y->X)= freq(Y,X)/freq(Y)
        * 5- find all the triples confidence value Conf(X,Y->Z) = freq(X,Y,Z)/freq(X, Y)
        * 6- Print out the results
        *
        * */
        new Apriori();
        Apriori.data = "src/main/resources/data/browsing.txt";
        Apriori.pass(1);
        Apriori.pass(2);
        Apriori.pass(3);
        Apriori.pairConfidence();
        Apriori.tripleConfidence();
        Apriori.printRulesAndConfidence(Apriori.secondPassConf, "PairRule.txt");
        Apriori.printRulesAndConfidence(Apriori.thirdPassConf, "TripleRule.txt");
    }

    static class Apriori {
        public static HashMap<String, Double> firstPassCounts = new HashMap<>();
        public static HashMap<List<String>, Double> secondPassCount = new HashMap<>();
        public static HashMap<List<String>, Double> thirdPassCount = new HashMap<>();
        public static String data;
        public static HashMap<List<String>, Double> secondPassConf = new HashMap<>();
        public static HashMap<List<String>, Double> thirdPassConf = new HashMap<>();


        public static void pass(Integer passNum) {
            try {
                Scanner scanner = new Scanner(new File(data));
                while (scanner.hasNextLine()) {
                    switch (passNum) {
                        case 1:
                            firstPass(scanner.nextLine());
                            break;
                        case 2:
                            secondPass(scanner.nextLine());
                            break;
                        case 3:
                            thirdPass(scanner.nextLine());
                            break;
                    }
                }
                scanner.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Apriori.prune(passNum);
        }

        public static void prune(Integer passNum) {
            switch (passNum) {
                case 1:
                    Iterator firstPassIter = firstPassCounts.keySet().iterator();
                    while (firstPassIter.hasNext()) {
                        if (firstPassCounts.get(firstPassIter.next())< 100.0) {
                            firstPassIter.remove();
                        }
                    }
                    break;
                case 2:
                    Iterator secondPassIter = secondPassCount.keySet().iterator();
                    while (secondPassIter.hasNext()) {
                        if (secondPassCount.get(secondPassIter.next()) < 100.0) {
                            secondPassIter.remove();
                        }
                    }
                    break;
                case 3:
                    Iterator thirdPassIter = thirdPassCount.keySet().iterator();
                    while (thirdPassIter.hasNext()) {
                        if (thirdPassCount.get(thirdPassIter.next()) < 100.0) {
                            thirdPassIter.remove();
                        }
                    }
                    break;
            }
        }


        public static void firstPass(String line) {
            String[] sessionItems = line.split("\\s");
            for (String item : sessionItems) {
                if (firstPassCounts.containsKey(item)) {
                    firstPassCounts.put(item, firstPassCounts.get(item) + 1.0);
                } else {
                    firstPassCounts.put(item, 1.0);
                }
            }
        }


        public static void secondPass(String line) {
            String[] sessionItems = line.split("\\s");
            for (String item1 : sessionItems) {
                for (String item2 : sessionItems) {
                    if (!item1.equals(item2)) {
                        if (firstPassCounts.containsKey(item1) && firstPassCounts.containsKey(item2)) {
                            List<String> pair = new ArrayList<>();
                            pair.add(item1);
                            pair.add(item2);
                            if (secondPassCount.containsKey(pair)) {
                                secondPassCount.put(pair, secondPassCount.get(pair) + 1.0);
                            } else {
                                secondPassCount.put(pair, 1.0);
                            }
                        }
                    }
                }
            }

        }

        public static void thirdPass(String line) {
            String[] sessionItems = line.split("\\s");
            for (String item1 : sessionItems) {
                for (String item2 : sessionItems) {
                    if (!item1.equals(item2)) {
                        if (firstPassCounts.containsKey(item1) && firstPassCounts.containsKey(item2)) {
                            List<String> sessionPairs = new ArrayList<>();
                            sessionPairs.add(item1);
                            sessionPairs.add(item2);
                            for (String item3 : sessionItems) {
                                if (secondPassCount.containsKey(sessionPairs) && firstPassCounts.containsKey(item3)
                                        && !item1.equals(item3) && !item2.equals(item3)) {
                                    List<String> triple = new ArrayList<>();
                                    triple.addAll(sessionPairs);
                                    triple.add(item3);
                                    if (thirdPassCount.containsKey(triple)) {
                                        thirdPassCount.put(triple, thirdPassCount.get(triple) + 1.0);
                                    } else {
                                        thirdPassCount.put(triple, 1.0);
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }

        public static void pairConfidence() {
            for (List<String> sc : secondPassCount.keySet()) {
                List<String> pair = sc;
                for (String item1 : pair) {
                    for (String item2 : pair) {
                        if (!item1.equals(item2)) {
                            List<String> pair1 = new ArrayList<>();
                            pair1.add(item1);
                            pair1.add(item2);
                            List<String> pair2 = new ArrayList<>();
                            pair2.add(item2);
                            pair2.add(item1);
                            if (secondPassCount.get(pair1) != null) {
                                double countPair = secondPassCount.get(pair1);
                                double leftCount = firstPassCounts.get(item1);
                                double confidence = countPair / leftCount;
                                secondPassConf.put(pair1, confidence);
                            }
                            if (secondPassCount.get(pair2) != null) {
                                double countPair = secondPassCount.get(pair2);
                                double leftCount = firstPassCounts.get(item2);
                                double confidence = countPair / leftCount;
                                secondPassConf.put(pair2, confidence);
                            }

                        }
                    }
                }


            }
        }

        public static void getRule(String item1, String item2, String item3) {
            List<String> left = new ArrayList<>();
            left.add(item1);
            left.add(item2);
            List<String> triple = new ArrayList<>();
            triple.addAll(left);
            triple.add(item3);
            if (secondPassCount.get(left) != null && thirdPassCount.get(triple) != null) {
                double countPair2 = secondPassCount.get(left);
                double countTriple2 = thirdPassCount.get(triple);
                double confidence2 = countTriple2 / countPair2;
                thirdPassConf.put(triple, confidence2);
            } else {
                thirdPassConf.put(triple, 0.0);
            }
        }

        public static void tripleConfidence() {
            for (List<String> tp : thirdPassCount.keySet()) {
                List<String> triple = tp;
                for (String item1 : triple) {
                    for (String item2 : triple) {
                        if (!item1.equals(item2)) {
                            for (String item3 : triple) {
                                if (!item2.equals(item3) && !item1.equals(item3)) {
                                    getRule(item1, item2, item3);
                                    getRule(item1, item3, item2);
                                    getRule(item2, item3, item1);
                                }
                            }
                        }

                    }
                }


            }
        }

        public static void printRulesAndConfidence(HashMap<List<String>, Double> rules, String outputFile) {
            try {
                FileWriter myWriter = new FileWriter(outputFile);
                Apriori.MyComparator comparator1 = new Apriori.MyComparator(rules);

                java.util.Map<List<String>, Double> sortedPass1 = new TreeMap<>(comparator1);
                sortedPass1.putAll(rules);

                Iterator<Map.Entry<List<String>, Double>> iter1 = sortedPass1.entrySet().iterator();
                Map.Entry<List<String>, Double> entry1;
                while (iter1.hasNext()) {
                    entry1 = iter1.next();
                    myWriter.write(entry1.getKey().toString() + "=" + entry1.getValue().toString() + "\n");
                }
                myWriter.close();
                System.out.println("Writing Finished Successfully.");
            } catch (IOException e) {
                System.out.println("Writing Error.");
                e.printStackTrace();
            }
        }


        static class MyComparator implements Comparator<Object> {

            HashMap<List<String>, Double> map;

            public MyComparator(HashMap<List<String>, Double> map) {
                this.map = map;
            }

            public int compare(Object o1, Object o2) {
                if ((map.get(o1) > map.get(o2)))
                    return -1;
                else
                    return 1;

            }
        }

    }
}
