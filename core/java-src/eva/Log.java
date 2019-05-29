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

import java.util.Map;

/**
 * Interface to the transaction log.
 */
public interface Log {
    /**
     * Given a {@link Log} object and a start and end 'transaction number' or 'id', return all
     * applicable datoms that were asserted or retracted in the database.
     *
     * @param startT Start transaction number
     * @param endT Ending transaction number
     * @return All datoms that were asserted or retracted between startT and endT
     */
    Iterable<Map> txRange(Object startT, Object endT);
}
