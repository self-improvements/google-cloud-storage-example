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

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.Lists;
import io.github.imsejin.common.util.CollectionUtils;
import io.github.imsejin.common.util.StringUtils;
import io.github.imsejin.gcstorage.constant.SearchPolicy;
import io.github.imsejin.gcstorage.exception.NoSuchBlobException;
import io.github.imsejin.gcstorage.util.MimeTypeUtils;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.http.client.utils.URIBuilder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * Helper for Google Cloud Storage(Firebase Storage).
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class Helper {

    private static final String TOKEN_KEY = "firebaseStorageDownloadTokens";

    private final String bucketName;

    private final Storage storage;

    ///////////////////////////////// Getters ///////////////////////////////////

    /**
     * Checks whether the blob exists or not.
     *
     * @param blob   blob
     * @param blobId id of the blob
     * @return blob
     * @throws NoSuchBlobException if the blob doesn't exist
     */
    private static Blob checkExistence(Blob blob, BlobId blobId) {
        if (blob == null || !blob.exists()) {
            throw new NoSuchBlobException("Could not find the blob: %s/%s", blobId.getBucket(), blobId.getName());
        }

        return blob;
    }

    private static void sortByDirectory(List<Blob> blobs, boolean dirFirst) {
        if (CollectionUtils.isNullOrEmpty(blobs) || blobs.size() == 1) return;

        Function<Blob, Boolean> orderByDirectory;
        if (dirFirst) {
            // 디렉터리 내림차순: [Directory, Directory, File, File]
            orderByDirectory = (Blob blob) -> !blob.isDirectory();
        } else {
            // 디렉터리 오름차순: [File, File, Directory, Directory]
            orderByDirectory = Blob::isDirectory;
        }

        blobs.sort(Comparator.comparing(orderByDirectory).thenComparing(Blob::getName));
    }

    @SneakyThrows
    private static void uploadToStorage(Storage storage, BlobInfo blobInfo, File file) {
        Path path = file.toPath();

        // For small files.
        if (file.length() < 1_000_000) {
            byte[] bytes = Files.readAllBytes(path);
            storage.create(blobInfo, bytes);

            return;
        }

        /*
         * For big files.
         * When content is not available or large (1MB or more),
         * it is recommended to write it in chunks via the blob's channel writer.
         */
        try (FileInputStream in = new FileInputStream(file);
             WriteChannel writableChannel = storage.writer(blobInfo)) {
            in.getChannel().transferTo(0, Long.MAX_VALUE, writableChannel);
        }
    }

    /**
     * Converts URL to name of blob
     *
     * <pre>
     * URL url = new URL("https://firebasestorage.googleapis.com/v0/b/steady-copilot-206205.appspot.com/o/goods%2F20180101%2Fimage_label1.jpeg?alt=media&token=65964335-7987-458e-9a79-d336e3ddc5ba");
     * toBlobName(url) // "goods/20180101/image_label1.jpeg"
     * </pre>
     *
     * @param url URL
     * @return name of blob
     */
    @SneakyThrows
    public static String toBlobName(URL url) {
        String path = url.getPath();
        int i = path.lastIndexOf('/');

        String encodedBlobName = path.substring(i + 1);
        return URLDecoder.decode(encodedBlobName, StandardCharsets.UTF_8.name());
    }

    /**
     * `버킷 내 파일경로`를 받아 파일명을 추출한다.
     *
     * <pre>
     * String blobName1 = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * toSimpleName(blobName1) // "5bf62022ff2e9e001090fba9_label1"
     *
     * String blobName2 = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1.jpg";
     * toSimpleName(blobName2) // "5bf62022ff2e9e001090fba9_label1.jpg"
     * </pre>
     */
    public static String toSimpleName(@NonNull String blobName) {
        int i = blobName.lastIndexOf('/');
        if (i == -1) return blobName;

        return blobName.substring(i + 1);
    }

    /**
     * Returns URL of the blob.
     *
     * <p> Please enable Firebase Storage for your bucket by visiting the Storage tab
     * in the Firebase Console and ensure that you have sufficient permission
     * to properly provision resources.
     *
     * <pre>
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * Blob blob = getBlob(blobName);
     *
     * toURL(blob) // "https://firebasestorage.googleapis.com/v0/b/steady-copilot-206205.appspot.com/o/goods%2F5bf62022ff2e9e001090fba9%2F5bf62022ff2e9e001090fba9_label1?alt=media&token=65964335-7987-458e-9a79-d336e3ddc5ba"
     * </pre>
     *
     * @param blob blob
     * @return URL of the blob
     */
    public static URL toURL(Blob blob) {
        String token = null;

        Map<String, String> meta = blob.getMetadata();
        if (meta != null && meta.containsKey("token")) {
            token = meta.get(TOKEN_KEY);
        }

        return generateURL(blob.getBucket(), blob.getName(), token);
    }

    /**
     * Returns URL of the blob.
     *
     * @param bucketName name of the bucket that contains the blob
     * @param blobName   name of the blob
     * @return URL of the blob
     */
    @SneakyThrows
    private static URL generateURL(String bucketName, String blobName) {
        return generateURL(bucketName, blobName, null);
    }

    /////////////////////////////////// Downloaders ///////////////////////////////////

    /**
     * Returns URL of the blob.
     *
     * @param bucketName name of the bucket that contains the blob
     * @param blobName   name of the blob
     * @param token      token for download
     * @return URL of the blob
     */
    @SneakyThrows
    private static URL generateURL(String bucketName, String blobName, @Nullable String token) {
        URIBuilder builder = new URIBuilder();

        // "https://firebasestorage.googleapis.com/v0/b/{bucketName}/o/{blobNameToBeEncoded}?alt=media"
        builder.setScheme("https")
                .setHost("firebasestorage.googleapis.com")
                .setPathSegments("v0", "b", bucketName, "o", blobName)
                .addParameter("alt", "media");

        // += "&token=b1d13c8d-505a-4d7a-8d1b-3ff20fb9d514"
        if (!StringUtils.isNullOrBlank(token)) builder.addParameter("token", token);

        return builder.build().toURL();
    }

    /**
     * Returns the blob.
     *
     * <pre>
     * String blobName = "goods/20180101/image_label1.jpeg";
     * Blob blob = getBlob(blobName);
     * </pre>
     *
     * @param blobName name of the blob
     * @return blob
     * @throws NoSuchBlobException if the blob doesn't exist
     */
    public Blob getBlob(@NonNull String blobName) {
        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);

        return checkExistence(blob, blobId);
    }

    /**
     * Return names of blobs selected with the specific way.
     *
     * <pre>
     * life-cycles/
     * ├─ 20201231/
     * │  ├─ market-kurly.zip
     * │  ├─ emart.zip
     * ├─ 20210129/
     * │  ├─ homeplus.zip
     *
     * ---
     *
     * // ["life-cycles/20201231/", "life-cycles/20210129/"]
     * getBlobNames("life-cycles/", SearchPolicy.DIRECTORIES)
     *
     * // ["life-cycles/20201231/market-kurly.zip", "life-cycles/20201231/emart.zip"]
     * getBlobNames("life-cycles/20201231/", SearchPolicy.FILES)
     * </pre>
     *
     * @param blobName name of blob
     * @param policy   how to select blobs
     * @return names of selected blobs
     */
    public List<String> getBlobNames(String blobName, @NonNull SearchPolicy policy) {
        List<Blob> blobs = getBlobs(blobName);

        return blobs.stream().filter(policy.getCondition())
                .filter(it -> !it.getName().equals(blobName)) // Except itself.
                .map(Blob::getName)
                .collect(toList());
    }

    /**
     * Selects blobs with the specific way.
     *
     * <pre>
     * life-cycles/
     * ├─ 20201231/
     * │  ├─ market-kurly.zip
     * │  ├─ emart.zip
     * ├─ 20210129/
     * │  ├─ homeplus.zip
     *
     * ---
     *
     * // [Blob(name="life-cycles/20201231/"), Blob(name="life-cycles/20210129/")]
     * getBlobs("life-cycles/", SearchPolicy.DIRECTORIES)
     *
     * // [Blob(name="life-cycles/20201231/market-kurly.zip"), Blob(name="life-cycles/20201231/emart.zip")]
     * getBlobs("life-cycles/20201231/", SearchPolicy.FILES)
     * </pre>
     *
     * @param blobName name of blob
     * @param policy   how to select blobs
     * @return selected blobs
     */
    public List<Blob> getBlobs(String blobName, @NonNull SearchPolicy policy) {
        List<Blob> blobs = getBlobs(blobName);

        return blobs.stream().filter(policy.getCondition())
                .filter(it -> !it.getName().equals(blobName)) // Except itself.
                .collect(toList());
    }

    /////////////////////////////////// Uploaders ///////////////////////////////////

    /**
     * 해당 `Blob`을 다운로드하거나, 디렉터리일 경우 최하위의 마지막 파일을 다운로드할 수 있는 URL을 가져온다.<br>
     * (같은 경로에 디렉터리와 파일이 있을 경우, 파일과 디렉터리 중 무엇을 우선으로 탐색할지 결정해야 한다.)
     *
     * <pre>
     * life-cycles/
     * ├─ 20201231/
     * │  ├─ market-kurly.zip
     * │  ├─ emart.zip
     * ├─ 20210129/
     * │  ├─ homeplus.zip
     * ├─ sample.zip
     *
     * ---
     *
     * String blobName = "life-cycles/";
     *
     * // 파일을 우선하여 선택.
     * getLastBlob(blobName, true) // Blob(name="life-cycles/sample.zip")
     *
     * // 디렉터리를 우선하여 선택.
     * getLastBlob(blobName, false) // Blob(name="life-cycles/20210129/homeplus.zip")
     * </pre>
     *
     * @param blobName    name of the blob
     * @param priorToFile Whether to choose a file first
     * @return the last blob
     */
    @Nullable
    public Blob getLastBlob(String blobName, boolean priorToFile) {
        List<Blob> blobs = getBlobs(blobName, SearchPolicy.ALL);
        if (CollectionUtils.isNullOrEmpty(blobs)) return null;

        sortByDirectory(blobs, priorToFile);
        Blob blob = blobs.get(blobs.size() - 1);

        if (blob.isDirectory()) {
            blob = getLastBlob(blob.getName(), priorToFile);
        }

        return blob == null || !blob.exists() ? null : blob;
    }

    private List<Blob> getBlobs(String blobName) {
        Page<Blob> blobPage = storage.list(bucketName, BlobListOption.currentDirectory(), BlobListOption.prefix(blobName));
        return Lists.newArrayList(blobPage.iterateAll()).stream().filter(Objects::nonNull).collect(toList());
    }

    /**
     * Downloads the blob and returns a file of the blob.
     *
     * <pre>
     * Path dest = Paths.get("/data", "product", "images");
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     *
     * download(blobName, dest) // File(path="/data/product/images/5bf62022ff2e9e001090fba9_label1")
     * </pre>
     *
     * @param blobName name of the blob
     * @param dest     destination
     * @return file of the blob
     */
    public File download(String blobName, Path dest) {
        Blob blob = getBlob(blobName);
        return download(blob, dest, null);
    }

    /////////////////////////////////// Modifiers ///////////////////////////////////

    /**
     * Downloads the blob and returns a file of the blob.
     *
     * <pre>
     * Path dest = Paths.get("/data", "product", "images");
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * String newFilename = "product_image.jpeg";
     *
     * download(blobName, dest, newFilename) // File(path="/data/product/images/product_image.jpeg")
     * </pre>
     *
     * @param blobName    name of the blob
     * @param dest        destination
     * @param newFilename name of the downloaded file
     * @return file of the blob
     */
    public File download(String blobName, Path dest, @Nullable String newFilename) {
        Blob blob = getBlob(blobName);
        return download(blob, dest, newFilename);
    }

    /**
     * Downloads the blob and returns a file of the blob.
     *
     * <pre>
     * Path dest = Paths.get("/data", "product", "images");
     * Blob blob = getBlob("goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1");
     *
     * download(blob, dest) // File(path="/data/product/images/5bf62022ff2e9e001090fba9_label1")
     * </pre>
     *
     * @param blob blob
     * @param dest destination
     * @return file of the blob
     */
    public File download(Blob blob, Path dest) {
        return download(blob, dest, null);
    }

    /**
     * Downloads the blob and returns a file of the blob.
     *
     * <pre>
     * Path dest = Paths.get("/data", "product", "images");
     * Blob blob = getBlob("goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1");
     * String newFilename = "product_image.jpeg";
     *
     * download(blob, dest, newFilename) // File(path="/data/product/images/product_image.jpeg")
     * </pre>
     *
     * @param blob        blob
     * @param dest        destination
     * @param newFilename name of the downloaded file
     * @return file of the blob
     */
    @SneakyThrows
    public File download(Blob blob, Path dest, @Nullable String newFilename) {
        // 새로운 파일명을 지정하지 않은 경우
        if (StringUtils.isNullOrBlank(newFilename)) newFilename = toSimpleName(blob.getName());

        Path path = Paths.get(dest.toString(), newFilename);

        Path directoryPath = path.getParent();
        if (Files.notExists(directoryPath)) Files.createDirectories(directoryPath);

        blob.downloadTo(path);

        return path.toFile();
    }

    /**
     * Uploads a file to storage.
     *
     * <pre>
     * String bucketName = "steady-copilot-206205.appspot.com";
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * BlobId blobId = BlobId.of(bucketName, blobName);
     * File file = new File("/data/product/images", "product_image.jpeg");
     *
     * upload(blobId, file);
     * </pre>
     *
     * @param blobId id of the blob
     * @param file   file to be uploaded
     */
    public void upload(BlobId blobId, File file) {
        upload(blobId, file, MimeTypeUtils.getMimeType(file));
    }

    /**
     * Uploads a file to storage with the specific MIME-Type.
     * If MIME-Type is empty, sets 'application/octet-stream'.
     *
     * <pre>
     * String bucketName = "steady-copilot-206205.appspot.com";
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * BlobId blobId = BlobId.of(bucketName, blobName);
     *
     * File file = new File("/data/product/images", "product_image.jpeg");
     * String mimeType = "image/jpeg";
     *
     * upload(blobId, file, mimeType);
     * </pre>
     *
     * @param blobId   id of the blob
     * @param file     file to be uploaded
     * @param mimeType MIME-Type of the file
     */
    public void upload(BlobId blobId, File file, @Nullable String mimeType) {
        Map<String, String> meta = new HashMap<>();
        meta.put(TOKEN_KEY, UUID.randomUUID().toString());

        String contentType = StringUtils.ifNullOrBlank(mimeType, "application/octet-stream");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType(contentType)
                .setMetadata(meta)
                .build();

        uploadToStorage(storage, blobInfo, file);
    }

    /////////////////////////////////// Converters ///////////////////////////////////

    /**
     * Moves the blob to the specific place.
     * Returns name of the moved blob, if failed to move, returns null.
     *
     * <pre>
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * String newBlobName = "goods_dev/cde94dc2-425c-8040/cde94dc2-425c-8040_img_etc1.jpg";
     *
     * move(blobName, newBlobName) // Blob(name="goods_dev/cde94dc2-425c-8040/cde94dc2-425c-8040_img_etc1.jpg")
     * </pre>
     *
     * @param blobName    old name of the blob
     * @param newBlobName new name of the blob
     * @return new name of the blob or null
     */
    public Blob move(@NonNull String blobName, String newBlobName) {
        Blob blob = getBlob(blobName);

        CopyWriter copyWriter = blob.copyTo(BlobId.of(bucketName, newBlobName));
        Blob copiedBlob = copyWriter.getResult();
        blob.delete();

        return copiedBlob;
    }

    /**
     * Moves the blob to the specific place.
     * Returns name of the moved blob, if failed to move, returns null.
     *
     * <pre>
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * Blob blob = getBlob(blobName);
     * String newBlobName = "goods_dev/cde94dc2-425c-8040/cde94dc2-425c-8040_img_etc1.jpg";
     *
     * move(blobName, newBlobName) // Blob(name="goods_dev/cde94dc2-425c-8040/cde94dc2-425c-8040_img_etc1.jpg")
     * </pre>
     *
     * @param blob        blob
     * @param newBlobName new name of the blob
     * @return new name of the blob or null
     */
    public Blob move(Blob blob, String newBlobName) {
        CopyWriter copyWriter = blob.copyTo(BlobId.of(bucketName, newBlobName));
        Blob copiedBlob = copyWriter.getResult();
        blob.delete();

        return copiedBlob;
    }

    /**
     * Renames the blob with the new name.
     * Returns new name of the blob, if failed to move, returns null.
     *
     * <pre>
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * String newSimpleName = "cde94dc2-425c-8040_img_etc1.jpg";
     *
     * rename(blobName, newSimpleName) // Blob(name="goods/5bf62022ff2e9e001090fba9/cde94dc2-425c-8040_img_etc1.jpg")
     * </pre>
     *
     * @param blobName      name of blob
     * @param newSimpleName new simple name of blob
     * @return new name of the blob or null
     */
    public Blob rename(@NonNull String blobName, String newSimpleName) {
        int i = blobName.lastIndexOf('/');

        String newBlobName;
        if (i == -1) {
            newBlobName = newSimpleName;
        } else {
            newBlobName = blobName.substring(0, i + 1) + newSimpleName;
        }

        return move(blobName, newBlobName);
    }

    /**
     * Renames the blob with the new name.
     * Returns new name of the blob, if failed to move, returns null.
     *
     * <pre>
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * String newSimpleName = "cde94dc2-425c-8040_img_etc1.jpg";
     *
     * rename(blobName, newSimpleName) // Blob(name="goods/5bf62022ff2e9e001090fba9/cde94dc2-425c-8040_img_etc1.jpg")
     * </pre>
     *
     * @param blob          blob
     * @param newSimpleName new simple name of blob
     * @return new name of the blob or null
     */
    public Blob rename(Blob blob, String newSimpleName) {
        String blobName = blob.getName();
        int i = blobName.lastIndexOf('/');

        String newBlobName;
        if (i == -1) {
            newBlobName = newSimpleName;
        } else {
            newBlobName = blobName.substring(0, i + 1) + newSimpleName;
        }

        return move(blob, newBlobName);
    }

    /**
     * Deletes the blob.
     *
     * @param blobName name of blob
     * @return whether the blob is successfully deleted
     * @Description :
     */
    public boolean delete(@NonNull String blobName) {
        Blob blob = storage.get(BlobId.of(bucketName, blobName));

        if (blob == null || !blob.exists()) return false;
        return blob.delete();
    }

    /**
     * Returns file extension of the blob.
     *
     * <pre>
     * // 확장자가 존재하는 경우
     * toFileExtension("goods/20201231/product_label1.jpg") // "jpg"
     *
     * // 확장자가 존재하는 않는 경우
     * toFileExtension("goods/20201231/product_label1") // image/jpeg -> "jpeg"
     * </pre>
     *
     * @param blobName name of blob
     * @return file extension of the blob
     * @throws NoSuchBlobException if the blob doesn't exist
     */
    public String toFileExtension(@NonNull String blobName) {
        if (StringUtils.isNullOrBlank(blobName)) return "";

        // 확장자가 존재하는 경우
        String simpleName = toSimpleName(blobName);
        if (simpleName.matches(".+\\..+$")) return simpleName.substring(simpleName.lastIndexOf('.') + 1);

        // MIME-Type을 확인할 수 없는 경우
        Blob blob = getBlob(blobName);
        String contentType = blob.getContentType();
        if (StringUtils.isNullOrBlank(contentType)) return "";

        return MimeTypeUtils.toExtension(contentType);
    }

    /**
     * Returns URL of the blob.
     *
     * <p> Please enable Firebase Storage for your bucket by visiting the Storage tab
     * in the Firebase Console and ensure that you have sufficient permission
     * to properly provision resources.
     *
     * <pre>
     * String blobName = "goods/5bf62022ff2e9e001090fba9/5bf62022ff2e9e001090fba9_label1";
     * toURL(blobName) // "https://firebasestorage.googleapis.com/v0/b/steady-copilot-206205.appspot.com/o/goods%2F5bf62022ff2e9e001090fba9%2F5bf62022ff2e9e001090fba9_label1?alt=media&token=65964335-7987-458e-9a79-d336e3ddc5ba"
     * </pre>
     *
     * @param blobName name of the blob
     * @return URL of the blob
     */
    public URL toURL(String blobName) {
        return generateURL(bucketName, blobName);
    }

    /**
     * Returns the blob.
     *
     * <pre>
     * URL url = new URL("https://firebasestorage.googleapis.com/v0/b/steady-copilot-206205.appspot.com/o/goods%2F20180101%2Fimage_label1.jpeg?alt=media&token=65964335-7987-458e-9a79-d336e3ddc5ba");
     * Blob blob = toBlob(url) // Blob(name="goods/20180101/image_label1.jpeg")
     * </pre>
     *
     * @param url URL of the blob
     * @return blob
     * @throws NoSuchBlobException if the blob doesn't exist
     */
    public Blob toBlob(URL url) {
        return getBlob(toBlobName(url));
    }

}