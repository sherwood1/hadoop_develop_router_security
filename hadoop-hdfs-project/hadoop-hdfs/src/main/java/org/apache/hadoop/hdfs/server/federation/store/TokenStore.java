/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.store;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.FederatedToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Management API for
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.FederatedToken
 *  FederatedToken} records in the state store. Accesses the data store via the
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.
 * StateStoreDriver StateStoreDriver} interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class TokenStore
    extends CachedRecordStore<FederatedToken> {

  public TokenStore(StateStoreDriver driver) {
    super(FederatedToken.class, driver);
  }

  /**
   * Add a federated token to the State Store.
   *
   * @param token Federated token.
   * @return If the token was added.
   * @throws IOException
   */
  public abstract boolean addToken(FederatedToken token) throws IOException;

  /**
   * Get all the federated tokens in the State Store.
   *
   * @return All the federated tokens.
   * @throws IOException
   */
  public abstract Collection<FederatedToken> getTokens() throws IOException;

  /**
   * Get the token in each name space for a federated token.
   *
   * @param token Token in the federation.
   * @return Name space -> Token.
   * @throws IOException
   */
  public abstract Map<String, Token<? extends TokenIdentifier>> getTokens(
      Token<? extends TokenIdentifier> token)  throws IOException;
}
