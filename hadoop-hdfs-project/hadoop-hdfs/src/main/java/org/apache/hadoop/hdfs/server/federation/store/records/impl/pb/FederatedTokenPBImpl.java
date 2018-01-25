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
package org.apache.hadoop.hdfs.server.federation.store.records.impl.pb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.FederatedTokenRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.FederatedTokenRecordProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.FederatedTokenRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.SubclusterTokenProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.FederatedToken;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the Federated Token record.
 */
public class FederatedTokenPBImpl extends FederatedToken
    implements PBRecord {

  private FederationProtocolPBTranslator<FederatedTokenRecordProto,
      FederatedTokenRecordProto.Builder, FederatedTokenRecordProtoOrBuilder>
      translator = new FederationProtocolPBTranslator<
      FederatedTokenRecordProto, FederatedTokenRecordProto.Builder,
      FederatedTokenRecordProtoOrBuilder>(
      FederatedTokenRecordProto.class);

  public FederatedTokenPBImpl() {
  }

  @Override
  public FederatedTokenRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }

  @Override
  public String getRouterId() {
    FederatedTokenRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasRouterId()) {
      return null;
    }
    return proto.getRouterId();
  }

  @Override
  public void setRouterId(String routerId) {
    Builder builder = this.translator.getBuilder();
    if (routerId == null) {
      builder.clearRouterId();
    } else {
      builder.setRouterId(routerId);
    }
  }

  @Override
  public Token<? extends TokenIdentifier> getToken() {
    FederatedTokenRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasToken()) {
      return null;
    }
    return convert(proto.getToken());
  }

  @Override
  public void setToken(Token<? extends TokenIdentifier> token) {
    Builder builder = this.translator.getBuilder();
    if (token == null) {
      builder.clearToken();
    } else {
      builder.setToken(PBHelper.convert(token));
    }
  }

  @Override
  public Map<String, Token<? extends TokenIdentifier>> getTokens() {
    Map<String, Token<? extends TokenIdentifier>> ret = new TreeMap<>();
    FederatedTokenRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    List<SubclusterTokenProto> tokens = proto.getTokensList();
    for (SubclusterTokenProto tokenProto : tokens) {
      String ns = tokenProto.getNameserviceId();
      TokenProto token = tokenProto.getToken();
      ret.put(ns, convert(token));
    }
    return ret;
  }

  @Override
  public void setTokens(
      Map<FederationNamespaceInfo, Token<? extends TokenIdentifier>> tokens) {
    Builder builder = this.translator.getBuilder();
    if (tokens != null) {
      for (Entry<FederationNamespaceInfo, Token<? extends TokenIdentifier>> entry :
          tokens.entrySet()) {
        FederationNamespaceInfo ns = entry.getKey();
        String nsId = ns.getNameserviceId();
        Token<? extends TokenIdentifier> token = entry.getValue();

        SubclusterTokenProto.Builder pairBuilder =
            SubclusterTokenProto.newBuilder();
        pairBuilder.setNameserviceId(nsId);
        pairBuilder.setToken(PBHelper.convert(token));
        builder.addTokens(pairBuilder.build());
      }
    }
  }

  @Override
  public Token<? extends TokenIdentifier> getToken(String nsId) {
    if (nsId == null) {
      return null;
    }
    Builder builder = this.translator.getBuilder();
    List<SubclusterTokenProto> tokens = builder.getTokensList();
    for (SubclusterTokenProto token : tokens) {
      if (nsId.equals(token.getNameserviceId())) {
        return convert(token.getToken());
      }
    }
    return null;
  }

  @Override
  public void setDateModified(long time) {
    this.translator.getBuilder().setDateModified(time);
  }

  @Override
  public long getDateModified() {
    return this.translator.getProtoOrBuilder().getDateModified();
  }

  @Override
  public void setDateCreated(long time) {
    this.translator.getBuilder().setDateCreated(time);
  }

  @Override
  public long getDateCreated() {
    return this.translator.getProtoOrBuilder().getDateCreated();
  }

  private static Token<? extends TokenIdentifier> convert(
      TokenProto token) {
    return new Token<DelegationTokenIdentifier>(
        token.getIdentifier().toByteArray(),
        token.getPassword().toByteArray(),
        new Text(token.getKind()),
        new Text(token.getService()));
  }
}
