/**
 * Copyright (c) 2021 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.couchdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.lightcouch.CouchDBClient;
import org.lightcouch.CouchDBProperties;
import org.lightcouch.Document;
import org.lightcouch.View;

import com.google.gson.JsonObject;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

public class CouchClient extends DB {

	private CouchDbClient dbClient;
	private int batchSize;
	private List<JsonObject> batchInsertList;

	@Override
	public void init() throws DBException {
		CouchDbProperties properties = new CouchDbProperties();
		//CouchDB host IP
		properties.setHost("127.0.0.1");
		//CouchDB port - default is 5984
		properties.setPort(5984);
		//CouchDB database name
		properties.setDbName("testdb");
		//Also set username and password here if required
		properties.setCreateDbIfNotExist(true);
		properties.setProtocol("http");
        properties.setUsername("Admin");
        properties.setPassword("root");
		Properties props = getProperties();
		batchInsertList = new ArrayList<JsonObject>();
		//batchsize is used in case of insertions
		batchSize = Integer.parseInt(props.getProperty("batchsize", "10000"));
		dbClient = new CouchDbClient(properties);
		super.init();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		JsonObject found = dbClient.find(JsonObject.class, key, "stale=ok");
		if (null == found)
			return Status.NOT_FOUND;
		if (fields != null) {
			JsonObject jsonObject = new JsonObject();
			jsonObject.add("_id", found.get("_id"));
			jsonObject.add("_rev", found.get("_rev"));
			for (String field : fields) {
				jsonObject.add(field, found.get(field));
			}
			result.put(found.get("_id").toString(), new ByteArrayByteIterator(jsonObject.toString().getBytes()));
		}
		return Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		View view = dbClient.view("_all_docs").startKeyDocId(startkey).limit(recordcount).includeDocs(true);
		HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
		List<JsonObject> list = view.query(JsonObject.class);
		if (fields != null) {
			for (JsonObject doc : list) {
				JsonObject jsonObject = new JsonObject();
				jsonObject.add("_id", doc.get("_id"));
				jsonObject.add("_rev", doc.get("_rev"));
				for (String field : fields) {
					jsonObject.add(field, doc.get(field));
				}
				resultMap.put(doc.get("_id").toString(), new ByteArrayByteIterator(jsonObject.toString().getBytes()));
			}
			result.add(resultMap);
		}
		for (HashMap<String, ByteIterator> map : result) {
			for (String key : map.keySet()) {
				System.out.println(map.get(key).toString());
			}
		}
		return Status.OK;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		JsonObject jsonObject = dbClient.find(JsonObject.class, key);
		if (null == jsonObject) {
			return Status.NOT_FOUND;
		}
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			jsonObject.addProperty(entry.getKey(), entry.getValue().toString());
		}
		dbClient.update(jsonObject);
		return Status.OK;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("_id", key);
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			jsonObject.addProperty(entry.getKey(), entry.getValue().toString());
		}
		if (batchSize == 1) {
			dbClient.save(jsonObject);
		} else {
			batchInsertList.add(jsonObject);
			if (batchInsertList.size() == batchSize) {
				dbClient.bulk(batchInsertList, false);
				batchInsertList.clear();
			}
		}
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		dbClient.remove(dbClient.find(Document.class, key));
		return Status.OK;
	}

}
