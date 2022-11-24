/*
 * MIT License
 *
 * Copyright (c) 2021 Im Sejin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.github.imsejin.gcstorage.config;

import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.SneakyThrows;

import java.io.File;
import java.io.InputStream;
import java.util.Objects;
import java.util.Set;

public final class GoogleCloudStorageConfig {

    // Directory to store user credentials.
    public static final FileDataStoreFactory FILE_DATA_STORE_FACTORY = initFileDataStoreFactory();

    // User credential of Google Cloud Storage.
    private static final String SERVICE_CREDENTIAL_PATHNAME = "json/credentials.json";

    // Client for Google Cloud Storage.
    public static final Storage STORAGE = initStorage();

    /**
     * Authorizes the installed application to access user's protected data.
     */
    @SneakyThrows
    private static GoogleCredentials authorize() {
        Set<String> scopes = Set.of(StorageScopes.DEVSTORAGE_FULL_CONTROL);
        InputStream in = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(SERVICE_CREDENTIAL_PATHNAME);

        return ServiceAccountCredentials
                .fromStream(Objects.requireNonNull(in))
                .createScoped(scopes);
    }

    @SneakyThrows
    private static Storage initStorage() {
        GoogleCredentials credential = authorize();
        return StorageOptions.newBuilder().setCredentials(credential).build().getService();
    }

    @SneakyThrows
    private static FileDataStoreFactory initFileDataStoreFactory() {
        // Directory to store user credentials.
        File dataDirectory = new File("/data/google-cloud-storage", "user-credentials");
        return new FileDataStoreFactory(dataDirectory);
    }

}
