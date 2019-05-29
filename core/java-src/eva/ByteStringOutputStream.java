// Copyright 2015-2019 Workiva Inc.
// 
// Licensed under the Eclipse Public License 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://opensource.org/licenses/eclipse-1.0.php
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eva;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream implementation that collects written bytes into an immutable ByteString
 */
public class ByteStringOutputStream extends OutputStream {
    ByteArrayOutputStream bout;

    public ByteStringOutputStream() {
        this.bout = new ByteArrayOutputStream(2048);
    }

    @Override
    public void write(int b) throws IOException {
        bout.write(b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        bout.write(bytes, off, len);
    }

    /**
     * @return an immutable ByteString containing the bytes written
     */
    public ByteString toByteString() {
        return ByteString.wrapping(this.bout.toByteArray());
    }
}
