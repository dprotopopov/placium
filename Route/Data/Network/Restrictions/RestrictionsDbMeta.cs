﻿/*
 *  Licensed to SharpSoftware under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for 
 *  additional information regarding copyright ownership.
 * 
 *  SharpSoftware licenses this file to you under the Apache License, 
 *  Version 2.0 (the "License"); you may not use this file except in 
 *  compliance with the License. You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

namespace Route.Data.Network.Restrictions
{
    /// <summary>
    /// Represents restriction db meta-data.
    /// </summary>
    public class RestrictionsDbMeta
    {
        /// <summary>
        /// Creates a new restriction db meta-object.
        /// </summary>
        internal RestrictionsDbMeta(string vehicle, RestrictionsDb db)
        {
            this.Vehicle = vehicle;
            this.RestrictionsDb = db;
        }

        /// <summary>
        /// Gets the vehicle.
        /// </summary>
        public string Vehicle { get; private set; }

        /// <summary>
        /// Gets the restrictions db.
        /// </summary>
        public RestrictionsDb RestrictionsDb { get; private set; }
    }
}