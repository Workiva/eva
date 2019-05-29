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

import java.util.Set;

/**
 * Provides Entity/Object-oriented access to database-entities.
 * Entities are lazy: value of attributes are retrieved on-demand when
 * {@link #get(Object)} or {@link #touch()} are called.
 * After retrieval attribute-values are cached inside the entity.
 *
 * <p>Entities are equal if they have the same entity-id and equivalent databases.
 */
public interface Entity {

    /**
     * Return the database value that backs this entity.
     */
    Database db();

    /**
     * Get the value of the attribute named by k.
     *
     * @param k keyword or colon-prefixed string (ie. ":person/name")
     * @return value(s) of that attribute, or null if none
     */
    Object get(Object k);

    /**
     * Return the key names of the attributes.
     */
    Set<Object> keySet();

    /**
     * Loads all attributes of the entity, recursively touching any component entities.
     *
     * @return the {@link Entity}
     */
    Entity touch();
}
