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

package eva.error.v1;

import clojure.lang.Keyword;

public interface IErrorCode {
    /**
     * Is this an unknown (unspecified) error code?
     */
    default boolean isUnspecified() {
        return false;
    }

    /**
     * Returns a long uniquely representing this ErrorCode.
     */
    long getCode();

    /**
     * Return an HTTP error code as a long appropriate to this ErrorCode
     */
    long getHttpErrorCode();
    String getScope();

    /**
     * Return the name of this ErrorCode as a String
     */
    String getName();

    /**
     * Return a description of this error code as a Clojure keyword.
     */
    Keyword getKey();

    /**
     * Get an explanation (the message) of the error.
     */
    String getExplanation();

    /**
     * Use to compare two error codes.
     *
     * @param e Another error code for comparison
     * @return True if the error codes are the same, or if the parent of this error code is {@code e}.
     */
    boolean is(IErrorCode e);
}
