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

public interface Datom {
    /**
     * Datom's entity-id.
     */
    Object e();

    /**
     * Datom's attribute-id.
     */
    Object a();

    /**
     * Datom's value.
     */
    Object v();

    /**
     * Datom's transaction number.
     */
    Object tx();

    /**
     * Indicates if the datom was added or retracted.
     *
     * @return boolean indicating the datom was added or retracted
     */
    Object added();

    /**
     * Positional getter; treats the datom as a tuple of: [e a v tx added].
     *
     * @param index numeric index of [0, 1 ,2 ,3, 4]
     * @return value at the position in the datom
     */
    Object getIndex(int index);

    /**
     * Associative getter; treats datom as a map with keys: [:e, :a, :v, :tx, :added].
     *
     * @param key key of the field to access
     * @return value at the field mapped to the key
     */
    Object getKey(Object key);
}
