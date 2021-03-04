/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acme.i19612;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

public class ReproduceIssue19612Test {
    static final int randomSuffix = (int)(Math.random() * 1000);
    static final String fsName = "testfs"+ randomSuffix;
    static final String fileName = "file" + randomSuffix + ".txt";
    static final byte[] content = "Hello world".getBytes(StandardCharsets.UTF_8);

    static DataLakeServiceClient client;
    static DataLakeFileClient fClient;
    @BeforeAll
    static void beforeAll() {
        StorageSharedKeyCredential creds = credentials();
        String endpoint = "https://" + creds.getAccountName() + ".dfs.core.windows.net";
        client = new DataLakeServiceClientBuilder()
                .credential(creds)
                .endpoint(endpoint)
                .buildClient();
        client.createFileSystem(fsName);

        fClient = client.getFileSystemClient(fsName).getFileClient(fileName);
        fClient.upload(new ByteArrayInputStream(content), content.length);

    }

    @AfterAll
    static void cleanup() {
        client.deleteFileSystem(fsName);
    }

    @Test
    void openQueryInputStream() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String query = "SELECT * from BlobStorage";
        try (InputStream in = fClient.openQueryInputStream(query)) {
            int c;
            while ((c = in.read()) >= 0) {
                baos.write(c);
            }
        }

        byte[] actual = baos.toByteArray();
        // this fails, see https://github.com/Azure/azure-sdk-for-java/issues/19612
        Assertions.assertThat(actual).containsExactly(content);
    }


    @Test
    void read() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // This works as expected
        fClient.read(baos);

        byte[] actual = baos.toByteArray();
        Assertions.assertThat(actual).containsExactly(content);
    }

    static StorageSharedKeyCredential credentials() {
        final String azureStorageAccountName = Objects.requireNonNull(System.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
                "Set AZURE_STORAGE_ACCOUNT_NAME env var");
        final String azureStorageAccountKey = Objects.requireNonNull(System.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
                "Set AZURE_STORAGE_ACCOUNT_KEY env var");

        return new StorageSharedKeyCredential(azureStorageAccountName, azureStorageAccountKey);

    }
}
