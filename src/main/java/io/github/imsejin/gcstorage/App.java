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

package io.github.imsejin.gcstorage;

import com.google.cloud.storage.Blob;
import io.github.imsejin.common.constant.DateType;
import io.github.imsejin.gcstorage.constant.SearchPolicy;
import io.github.imsejin.gcstorage.core.Helper;
import io.github.imsejin.gcstorage.core.HelperFactory;

import java.time.LocalDate;
import java.util.List;

public class App {

    public static void main(String[] args) {
        String bucketName = "steady-copilot-206205.appspot.com";
        Helper helper = HelperFactory.create(bucketName);

        String today = LocalDate.now().format(DateType.F_DATE.getFormatter());
        String blobName = String.format("lifecycle-images/%s/", today);
        List<Blob> blobs = helper.getBlobs(blobName, SearchPolicy.ALL);
        blobs.forEach(System.out::println);
    }

}
