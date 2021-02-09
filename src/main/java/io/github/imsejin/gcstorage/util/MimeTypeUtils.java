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

package io.github.imsejin.gcstorage.util;

import lombok.SneakyThrows;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MimeTypes;

import java.io.File;

public final class MimeTypeUtils {

    private static final Tika tika = new Tika();
    private static final MimeTypes mimeTypes = TikaConfig.getDefaultConfig().getMimeRepository();

    private MimeTypeUtils() {
    }

    @SneakyThrows
    public static String getMimeType(File file) {
        return tika.detect(file);
    }

    /**
     * Converts MIME Type to file extension corresponding the MIME Type.
     *
     * <pre>
     *     toExtension("image/jpeg") // "jpeg"
     *     toExtension("application/gzip") // "gz"
     *     toExtension("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") // "xlsx"
     * </pre>
     *
     * @param mimeType MIME Type
     * @return file extension corresponding the MIME Type
     */
    @SneakyThrows
    public static String toExtension(String mimeType) {
        return mimeTypes.forName(mimeType).getExtension().replace(".", "");
    }

}
