package org.apache.hadoop.hdfs.server.federation.store.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
//import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.TokenStore;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.FederatedToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link RouterStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TokenStoreImpl extends TokenStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(TokenStoreImpl.class);


  /** Locally cached tokens. */
  private final Map<Token<? extends TokenIdentifier>, FederatedToken> tokens;

  /** Lock to access the local memory cache. */
  private final ReadWriteLock cacheReadWriteLock =
      new ReentrantReadWriteLock();
  private final Lock cacheReadLock = cacheReadWriteLock.readLock();
  private final Lock cacheWriteLock = cacheReadWriteLock.writeLock();


  public TokenStoreImpl(StateStoreDriver driver) {
    super(driver);
    this.tokens = new HashMap<>();
  }

  @Override
  public boolean addToken(FederatedToken token) throws IOException {
    LOG.info("Adding token {} to the store with {} tokens",
        token.getToken(), this.tokens.size());
    cacheWriteLock.lock();
    try {
      this.tokens.put(token.getToken(), token);
    } finally {
      cacheWriteLock.unlock();
    }
    return getDriver().put(token, true, false);
  }

  @Override
  public Collection<FederatedToken> getTokens() throws IOException {
    return this.tokens.values();
  }

  @Override
  public Map<String, Token<? extends TokenIdentifier>> getTokens(
      Token<? extends TokenIdentifier> token) throws IOException {

    // Get the token from the local cache
    Map<String, Token<? extends TokenIdentifier>> ret = null;
    cacheReadLock.lock();
    try {
      FederatedToken federatedToken = this.tokens.get(token);
      if (federatedToken != null) {
        Map<String, Token<? extends TokenIdentifier>> federatedTokens =
            federatedToken.getTokens();
        ret = new HashMap<String, Token<? extends TokenIdentifier>>(
            federatedTokens);
      }
    } finally {
      cacheReadLock.unlock();
    }

    return ret;
  }

  @Override
  public boolean loadCache(boolean force) throws IOException {
    super.loadCache(force);

    LOG.debug("Loading tokens...");

    List<FederatedToken> cachedRecords = getCachedRecords();
    if (cachedRecords != null) {
      // TODO Remove old tokens

      // Update the local cache with the State Store information
      cacheWriteLock.lock();
      try {
        this.tokens.clear();
        for (FederatedToken record : cachedRecords) {
          this.tokens.put(record.getToken(), record);
        }
      } finally {
        cacheWriteLock.unlock();
      }

      LOG.debug("Loaded {} federated tokens", this.tokens.size());
    }
    return true;
  }
}
