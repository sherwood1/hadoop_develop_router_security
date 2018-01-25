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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Data schema for a token in the federation.
 */
public abstract class FederatedToken extends BaseRecord implements PBRecord {

  /**
   * Default constructor for a mount table entry.
   */
  public FederatedToken() {
    super();
  }

  public static FederatedToken newInstance() {
    FederatedToken record =
        StateStoreSerializer.newRecord(FederatedToken.class);
    record.init();
    return record;
  }

  public static FederatedToken newInstance(
      String routerId, Token<DelegationTokenIdentifier> token) {
    FederatedToken record = FederatedToken.newInstance();
    record.setRouterId(routerId);
    record.setToken(token);
    return record;
  }

  /**
   * Set the identifier of the Router that generated the federated token.
   * @param routerId Identifier of the Router.
   */
  public abstract void setRouterId(String routerId);

  /**
   * Get the identifier of the Router that generated the federated token.
   * @return Identifier of the Router.
   */
  public abstract String getRouterId();

  /**
   * Get the main token in the federation.
   * @return Federated token.
   */
  public abstract Token<? extends TokenIdentifier> getToken();

  /**
   * Set the main token in the federation.
   * @param token Federated token.
   */
  public abstract void setToken(Token<? extends TokenIdentifier> token);

  /**
   * Get the delegation token in each subcluster.
   * @return Map between subcluster and delegation token.
   */
  public abstract Map<String, Token<? extends TokenIdentifier>> getTokens();

  /**
   * Set the delegation token in each subcluster.
   * @param tokens Map between subcluster and delegation token.
   */
  public abstract void setTokens(
      Map<FederationNamespaceInfo, Token<? extends TokenIdentifier>> tokens);

  /**
   * Get the token for a particular namespace.
   * @param nsId Namespace identifier.
   * @return Token for the namespace.
   */
  public abstract Token<? extends TokenIdentifier> getToken(String nsId);

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("routerId", this.getRouterId());
    map.put("token", new String(this.getToken().getIdentifier()));
    return map;
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(this.getToken())
        .append(this.getRouterId())
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FederatedToken) {
      FederatedToken other = (FederatedToken)obj;
      if (!this.getToken().equals(other.getToken())) {
        return false;
      } else if (!this.getRouterId().equals(other.getRouterId())) {
        return false;
      }
      return true;
    }
    return false;
  }
}