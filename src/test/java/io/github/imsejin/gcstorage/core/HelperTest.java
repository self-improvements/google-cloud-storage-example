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

package io.github.imsejin.gcstorage.core;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import io.github.imsejin.common.util.DateTimeUtils;
import io.github.imsejin.common.util.FilenameUtils;
import io.github.imsejin.common.util.StringUtils;
import io.github.imsejin.gcstorage.constant.SearchPolicy;
import io.github.imsejin.gcstorage.util.MimeTypeUtils;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;

import java.io.File;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.*;

class HelperTest {

    private static final String BUCKET_NAME = "steady-copilot-206205.appspot.com";

    private static final Helper helper = HelperFactory.create(BUCKET_NAME);

    @Test
    void getBlob() {
        // given
        String blobName = "user_data/db_list/topic/db_list_20200320_hand_wash.xlsx";

        // when
        Blob blob = helper.getBlob(blobName);

        // then
        assertThat(blob)
                .isNotNull()
                .returns(true, Blob::exists)
                .returns(blobName, Blob::getName);
    }

    @Test
    void getFileBlobs() {
        // given
        String blobName = "user_data/db_list/topic/";

        // when
        List<Blob> blobs = helper.getBlobs(blobName, SearchPolicy.FILES);

        // then
        assertThat(blobs).isNotNull();
        if (!blobs.isEmpty()) {
            assertThat(blobs)
                    .as("The selected blobs must contain of files.")
                    .allMatch(Predicate.not(Blob::isDirectory));
        }
    }

    @Test
    void getDirectoryBlobs() {
        // given
        String blobName = "user_data/db_list/";

        // when
        List<Blob> blobs = helper.getBlobs(blobName, SearchPolicy.DIRECTORIES);

        // then
        assertThat(blobs).isNotNull();
        if (!blobs.isEmpty()) {
            assertThat(blobs)
                    .as("The selected blobs must contain of directories.")
                    .allMatch(Blob::isDirectory);
        }
    }

    @Test
    void getBlobNames() {
        // given
        String blobName = "user_data/db_list/topic/";

        // when
        List<String> blobNames = helper.getBlobNames(blobName, SearchPolicy.FILES);

        // then
        assertThat(blobNames).isNotNull();
        if (!blobNames.isEmpty()) {
            assertThat(blobNames)
                    .as("Names of the selected blobs shouldn't be empty.")
                    .allMatch(Predicate.not(StringUtils::isNullOrBlank));
        }
    }

    @Test
    void getLastBlobPriorToFile() {
        // given
        String blobName = "user_data/db_list/2020/";

        // when
        Blob blob = helper.getLastBlob(blobName, true);

        // then
        assertThat(blob).isNotNull();
    }

    @Test
    void getLastBlobPriorToDirectory() {
        // given
        String blobName = "user_data/db_list/2020/";

        // when
        Blob blob = helper.getLastBlob(blobName, false);

        // then
        assertThat(blob).isNotNull();
    }

    @Test
    void toBlobName() {
        // given
        String blobName = "user_data/db_list/topic/db_list_20200320_immune.xlsx";

        // when
        Blob blob = helper.getBlob(blobName);
        URL url = helper.toURL(blob.getName());
        String name = Helper.toBlobName(url);

        // then
        assertThat(name)
                .isNotBlank()
                .isEqualTo(blobName);
    }

    @Test
    void toSimpleName() {
        // given
        String filename = "db_list_20200320_instant.xlsx";
        String blobName = "user_data/db_list/topic/" + filename;

        // when
        String simpleName = Helper.toSimpleName(blobName);

        // then
        assertThat(simpleName)
                .isNotBlank()
                .isEqualTo(filename);
    }

    @Test
    @SneakyThrows
    void toURL() {
        // given
        String blobName = "user_data/db_list/topic/db_list_20200320_mask.xlsx";

        // when
        URL url = helper.toURL(blobName);

        // then:1
        String encodedBlobName = URLEncoder.encode(blobName, StandardCharsets.UTF_8);
        assertThat(url)
                .isNotNull()
                .hasProtocol("https")
                .hasHost("firebasestorage.googleapis.com")
                .hasPath(String.format("/v0/b/%s/o/%s", BUCKET_NAME, encodedBlobName))
                .hasParameter("alt", "media");

        // then:2
        Blob blob = helper.toBlob(url);
        URLConnection connection = url.openConnection();
        assertThat(blob)
                .isNotNull()
                .returns(connection.getContentType(), Blob::getContentType)
                .returns(connection.getContentLengthLong(), Blob::getSize);
    }

    @Test
    void toFileExtension() {
        // given
        String blobName = "board-attachments/20200325/opensurvey_trend_wellness_2019_202002125101815.pdf";

        // when
        String extension = helper.toFileExtension(blobName);

        // then
        assertThat(extension)
                .isNotBlank()
                .isEqualTo(FilenameUtils.extension(new File(blobName)));
    }

    @Test
    @SneakyThrows
    void testToURL() {
        // given
        String blobName = "board-attachments/20200325/opensurvey_trend_online_grocery_2019_202002125101533.pdf";

        // when
        Blob blob = helper.getBlob(blobName);
        URL url = Helper.toURL(blob);

        // then:1
        String encodedBlobName = URLEncoder.encode(blobName, StandardCharsets.UTF_8);
        assertThat(url)
                .isNotNull()
                .hasProtocol("https")
                .hasHost("firebasestorage.googleapis.com")
                .hasPath(String.format("/v0/b/%s/o/%s", BUCKET_NAME, encodedBlobName))
                .hasParameter("alt", "media");

        // then:2
        URLConnection connection = url.openConnection();
        assertThat(blob)
                .isNotNull()
                .returns(connection.getContentType(), Blob::getContentType)
                .returns(connection.getContentLengthLong(), Blob::getSize);
    }

    @Test
    void toBlob() {
        // given
        Blob lastBlob = helper.getLastBlob("app_img/", true);
        assertThat(lastBlob).isNotNull();
        URL url = Helper.toURL(lastBlob);

        // when
        Blob blob = helper.toBlob(url);

        // then
        assertThat(blob).isEqualTo(lastBlob);
    }

    @Test
    void downloadAsOriginName() {
        // given
        String blobName = "user_data/db_list/2019";
        Blob blob = helper.getLastBlob(blobName, true);
        assertThat(blob).isNotNull();
        Path dest = Paths.get("/data", "google-cloud-storage", "downloads");

        // when
        File file = helper.download(blob, dest);

        // then
        assertThat(file)
                .isNotEmpty()
                .exists()
                .hasName(Helper.toSimpleName(blob.getName()))
                .hasSize(blob.getSize())
                .hasBinaryContent(blob.getContent());
    }

    @Test
    void downloadAsNewName() {
        // given
        String blobName = "user_data/db_list/2021";
        Blob blob = helper.getLastBlob(blobName, true);
        assertThat(blob).isNotNull();
        Path dest = Paths.get("/data", "google-cloud-storage", "downloads");

        // when
        String extension = helper.toFileExtension(blob.getName());
        String newFilename = String.format("db_list_%s.%s", DateTimeUtils.now(), extension);
        File file = helper.download(blob, dest, newFilename);

        // then
        assertThat(file)
                .isNotEmpty()
                .exists()
                .hasName(newFilename)
                .hasSize(blob.getSize())
                .hasBinaryContent(blob.getContent());
    }

    @Test
    void uploadSmallFile() {
        // given
        Blob blob = helper.getLastBlob("user_data/db_list/2021", true);
        Path dest = Paths.get("/data", "google-cloud-storage", "downloads");
        File file = helper.download(blob, dest);

        // when
        BlobId blobId = BlobId.of(BUCKET_NAME, "test/uploaded-small-file." + FilenameUtils.extension(file));
        helper.upload(blobId, file);

        // then
        Blob actual = helper.getBlob(blobId.getName());
        assertThat(actual)
                .isNotNull()
                .returns(true, Blob::exists)
                .returns(file.length(), Blob::getSize)
                .returns(MimeTypeUtils.getMimeType(file), Blob::getContentType);
    }

    @Test
    void uploadBigFile() {
        // given
        Blob blob = helper.getLastBlob("lifecycle-images/.processed/", true);
        Path dest = Paths.get("/data", "google-cloud-storage", "downloads");
        File file = helper.download(blob, dest);

        // when
        BlobId blobId = BlobId.of(BUCKET_NAME, "test/uploaded-big-file." + FilenameUtils.extension(file));
        helper.upload(blobId, file);

        // then
        Blob actual = helper.getBlob(blobId.getName());
        assertThat(actual)
                .isNotNull()
                .returns(true, Blob::exists)
                .returns(file.length(), Blob::getSize)
                .returns(MimeTypeUtils.getMimeType(file), Blob::getContentType);
    }

}
