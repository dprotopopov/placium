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

using OsmSharp;

namespace Route.IO.Osm.Streams
{
    /// <summary>
    /// Abstract representation of two-pass based osm-data processor.
    /// </summary>
    public interface ITwoPassProcessor
    {
        /// <summary>
        /// Processes the first pass of this node.
        /// </summary>
        void FirstPass(Node node);

        /// <summary>
        /// Processes the first pass of this way.
        /// </summary>
        void FirstPass(Way way);

        /// <summary>
        /// Processes the first pass of this relation.
        /// </summary>
        bool FirstPass(Relation relation);

        /// <summary>
        /// Processes a node in the second pass.
        /// </summary>
        void SecondPass(Node node);

        /// <summary>
        /// Processes a way in the second pass.
        /// </summary>
        void SecondPass(Way way);

        /// <summary>
        /// Processes a relation in a second pass.
        /// </summary>
        void SecondPass(Relation relation);
    }
}