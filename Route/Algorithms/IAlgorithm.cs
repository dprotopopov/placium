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

using System.Threading;

namespace Route.Algorithms
{
    /// <summary>
    /// Abstract representation of an algorithm.
    /// </summary>
    public interface IAlgorithm
    {
        /// <summary>
        /// Returns true if this instance has run already.
        /// </summary>
        bool HasRun
        {
            get;
        }

        /// <summary>
        /// Returns true if this instance has run and it was succesfull.
        /// </summary>
        bool HasSucceeded
        {
            get;
        }

        /// <summary>
        /// Runs the algorithm.
        /// </summary>
        void Run();

        /// <summary>
        /// Runs the algorithm.
        /// </summary>
        void Run(CancellationToken cancellationToken);

        /// <summary>
        /// Returns an error message when the algorithm was not successful.
        /// </summary>
        string ErrorMessage
        {
            get;
        }
    }
}