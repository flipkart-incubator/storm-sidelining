package com.flipkart.storm.sidelining.hbase.key;

import java.nio.charset.Charset;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.IntMath;

public interface KeyDistributor {

    String distributorEnrich (String key);

    int partitionHint (String key);

    class NoKeyDistributor implements KeyDistributor {

        @Override
        public String distributorEnrich(String key) {
            return key;
        }

        @Override
        public int partitionHint(String key) {
            return 0;
        }

    }

    class MurmurKeyDistributor implements KeyDistributor {
        private static final HashFunction hashFunction = Hashing.murmur3_32();

        private final int bucketCount;

        public MurmurKeyDistributor(int bucketCount) {
            this.bucketCount = bucketCount;
        }

        @Override
        public String distributorEnrich(String key) {
            int hint = partitionHint(key);
            return new StringBuilder().append(hint).append("-").append(key).toString();
        }

        @Override
        public int partitionHint(String key) {
            int hashInt = hashFunction.hashString(key, Charset.defaultCharset()).asInt();
            return IntMath.mod(hashInt, bucketCount);
        }
    }

}
